"""General transducers and accompanying functions.

Transducers are composable algorithmic transformations.

Transducer features:

- Are decoupled from the context in which they are used. This means they can be
  reused with any transducible process, including iterables and channels.
- Are composable with simple function composition. See comp().
- Support early termination via reduced values. See reduced().

Creating transducers:
Transducers are also known as reducing function transformers. They are simply
functions that accept a reducing function as input and return a new reducing
function as output. A reducing function is defined as a function that accepts
2 arguments (step arity), 1 argument (completion arity), and optionally
0 arguments (init arity). multi_arity() can be used to help create these
multi-arity reducing functions.

See https://clojure.org/reference/transducers for more information about
transducers.
"""

import functools as _functools
import itertools as _itertools
import random as _random
from collections import deque as _deque


class _Undefined:
    """A default parameter value that a user could never pass in."""


def identity(x):
    """A NOP transducer that simply returns its argument."""
    return x


def comp(*xforms):
    """Returns a new transducer equal to the composition of xforms.

    The returned transducer passes values through the given transformations
    from left to right.

    Args:
        xforms: Transducers.
    """
    return _functools.reduce(lambda f, g: lambda x: f(g(x)), xforms, identity)


def multi_arity(*funcs):
    """Returns a new multi-arity function which dispatches to funcs.

    The returned function will dispatch to the provided functions based on the
    number of positional arguments it was called with. If called with zero
    arguments it will dispatch to the first function in funcs, if called with
    one argument it will dispatch to the second function in funcs, etc.

    Args:
        funcs: Functions to dispatch to. Each function represents a different
            arity for the returned function. None values may be used to
            represent arities that don't exist.
    """
    def dispatch(*args):
        try:
            func = funcs[len(args)]
            if func is None:
                raise IndexError
        except IndexError:
            raise TypeError(f'wrong number of arguments, got {len(args)}')
        return func(*args)
    return dispatch


class _Reduced:
    def __init__(self, value):
        self.value = value


def reduced(x):
    """Wraps x in such a way that a reduce will terminate with x."""
    return _Reduced(x)


def is_reduced(x):
    """Returns True if x is the result from a call to reduced."""
    return isinstance(x, _Reduced)


def ensure_reduced(x):
    """Returns reduced(x) if x is not already a reduced value."""
    return x if is_reduced(x) else reduced(x)


def unreduced(x):
    """Returns x if it's not a reduced value else returns the unwrapped value."""
    return x.value if is_reduced(x) else x


def completing(rf, cf=identity):
    """Returns a wrapper around rf that calls cf when invoked with one argument.

    Args:
        rf: A reducing function.
        cf: An optional function that accepts a single argument. Used as the
            completion arity for the returned reducing function.

    Returns: A reducing function that dispatches to cf when called with a
        single argument or rf when called with any other number of arguments.
    """
    @_functools.wraps(rf)
    def wrapper(*args):
        if len(args) == 1:
            return cf(*args)
        return rf(*args)
    return wrapper


def _ireduce(rf, init, coll):
    result = init
    for x in coll:
        result = rf(result, x)
        if is_reduced(result):
            return unreduced(result)
    return result


def ireduce(rf, init, coll=_Undefined):
    """
    ireduce(rf, coll) -> reduction result
    ireduce(rf, init, coll) -> reduction result

    Returns the result of reducing an iterable.

    Reduces coll by repeatedly calling rf with 2 arguments. If coll is empty,
    then init will be returned. If coll is not empty, then the first call to rf
    will be rf(init, first_coll_element). rf will continue to get called as
    rf(prev_rf_return_value, next_coll_element) until either coll is exhausted
    or rf returns a reduced value.

    Args:
        rf: A reducing function accepting 2 arguments. If init is not provided,
            then rf must return a value to be used as init when called with 0
            arguments.
        init: An optional initial value.
        coll: An iterable.

    See Also:
        reduced
        itransduce
    """
    if coll is _Undefined:
        return _ireduce(rf, rf(), init)
    return _ireduce(rf, init, coll)


def _itransduce(xform, rf, init, coll):
    xrf = xform(rf)
    return xrf(ireduce(xrf, init, coll))


def itransduce(xform, rf, init, coll=_Undefined):
    """
    itransduce(xform, rf, coll) -> reduction result
    itransduce(xform, rf, init, coll) -> reduction result

    Returns the result of reducing an iterable with a transformation.

    Reduces coll using a transformed reducing function equal to xform(rf). See
    ireduce() for more information on reduction. After the transformed reducing
    function has received all input it will be called once more with a single
    argument, the result thus far.

    Args:
        xform: A transducer.
        rf: A reducing function accepting both 1 and 2 arguments. If init is
            not provided, then rf must return a value to be used as init when
            called with 0 arguments.
        init: An optional initial value.
        coll: An iterable.

    See Also:
        ireduce
    """
    if coll is _Undefined:
        return _itransduce(xform, rf, rf(), init)
    return _itransduce(xform, rf, init, coll)


def append(appendable=_Undefined, val=_Undefined):
    """
    append() -> []
    append(appendable) -> appendable
    append(appendable, val) -> appendable.append(val)

    A reducing function that appends val to appendable.
    """
    if appendable is _Undefined:
        return []
    if val is _Undefined:
        return appendable
    appendable.append(val)
    return appendable


def into(appendable, xform, coll):
    """Transfers all values from coll into appendable with a transformation.

    Same as itransduce(xform, append, appendable, coll).
    """
    return itransduce(xform, append, appendable, coll)


def xiter(xform, coll):
    """Returns an iterator over the transformed elements in coll.

    Useful for when you want to transform an iterable into another iterable
    in a lazy fashion.

    Args:
        xform: A transducer.
        coll: A potentially infinite iterable.
    """
    buffer = _deque()

    def flush_buffer(buf):
        assert buf is buffer, 'xform returned invalid value'
        while len(buf) > 0:
            yield buf.popleft()

    xrf = xform(append)
    for x in coll:
        ret = xrf(buffer, x)
        yield from flush_buffer(unreduced(ret))
        if is_reduced(ret):
            break

    yield from flush_buffer(xrf(buffer))


def _step_safety(step):
    """A decorator for step functions to help with debugging reduced cases.

    Args:
        step: A reducing function that accepts 2 arguments.

    Returns: A wrapper function that adds an assertion that the step function
        will never get called again once a reduced value is returned.
    """
    end_of_input = False

    @_functools.wraps(step)
    def safe_step(result, val):
        nonlocal end_of_input
        assert not end_of_input, 'step cannot get called after reduced value is returned'
        ret = step(result, val)
        if is_reduced(ret):
            end_of_input = True
        return ret

    return safe_step


def map(f):
    """Returns a transducer that applies f to each input.

    Args:
        f: A function accepting a single argument.
     """
    return lambda rf: multi_arity(rf, rf, lambda result, val: rf(result, f(val)))


def map_indexed(f):
    """Returns a transducer that transforms using f(index, value).

    The returned transducer applies f to each value with the corresponding
    index. f will be called as f(index, value) where index represents the nth
    value to be passed into the transformation starting at 0.

    Args:
        f: A function accepting index and value as arguments.
    """
    def xform(rf):
        i = -1

        def step(result, val):
            nonlocal i
            i += 1
            return rf(result, f(i, val))

        return multi_arity(rf, rf, step)
    return xform


def filter(pred):
    """Returns a transducer that outputs values for which predicate returns true.

    Args:
        pred: A predicate function that accepts a single value and returns
            true to keep it or false to remove it.

    See Also:
        filter_indexed
        remove
    """
    return lambda rf: multi_arity(rf, rf,
                                  lambda result, val: (rf(result, val)
                                                       if pred(val)
                                                       else result))


def filter_indexed(pred):
    """Returns a transducer which filters values based on pred(index, value).

    The returned transducer outputs values that return true when passed into
    pred with the corresponding index. pred will be called as
    pred(index, value) where index represents the nth value to be passed into
    the transformation starting at 0.

    Args:
        pred: A predicate function to be called as pred(index, value).

    See Also:
        filter
        remove_indexed
    """
    return comp(map_indexed(lambda i, x: x if pred(i, x) else _Undefined),
                filter(lambda x: x is not _Undefined))


def remove(pred):
    """Returns a transducer that drops values for which predicate returns true.

    Args:
        pred: A predicate function.

    See Also:
        filter
        remove_indexed
    """
    return filter(lambda x: not pred(x))


def remove_indexed(pred):
    """Returns a transducer which drops values based on pred(index, value).

    The returned transducer drops values that return true when passed into pred
    with the corresponding index. pred will be called as pred(index, value)
    where index represents the nth value to be passed into the transformation
    starting at 0.

    Args:
        pred: A predicate function to be called as pred(index, value).

    See Also:
        filter_indexed
        remove
    """
    return filter_indexed(lambda i, x: not pred(i, x))


def keep(f):
    """Returns a transducer that outputs the non-None return values of f(value).

    Args:
        f: A function accepting a single argument.

    See Also:
        keep_indexed
    """
    return comp(map(f), filter(lambda x: x is not None))


def keep_indexed(f):
    """Returns a transducer that outputs the non-None return values of f(index, value).

    The returned transducer outputs the non-None return values of
    f(index, value) where index represents the nth value to be passed into the
    transformation starting at 0.

    Args:
        f: A function to be called as f(index, value).

    See Also:
        keep
    """
    return comp(map_indexed(f), filter(lambda x: x is not None))


def cat(rf):
    """A transducer that concatenates the contents of its inputs.

    Expects each input to be an iterable, the contents of which will be
    outputted one at a time.

    Args:
        rf: A reducing function.
    """
    def double_reduced_rf(result, val):
        ret = rf(result, val)
        return reduced(ret) if is_reduced(ret) else ret

    return multi_arity(rf, rf, _functools.partial(ireduce, double_reduced_rf))


def mapcat(f):
    """Returns a transducer that applies f to each input and concatenates the result.

    Args:
        f: A function of the form, f(x) -> iterable.
    """
    return comp(map(f), cat)


def take(n):
    """Returns a transducer that outputs the first n inputs.

    The returned transducer outputs the first n inputs if n < the number of
    inputs. If n >= the number of inputs, then outputs all of them.

    Args:
        n: A number.
    """
    def xform(rf):
        remaining = n

        @_step_safety
        def step(result, val):
            nonlocal remaining
            new_result = rf(result, val) if remaining > 0 else result
            remaining -= 1
            return ensure_reduced(new_result) if remaining <= 0 else new_result

        return multi_arity(rf, rf, step)
    return xform


def take_last(n):
    """Returns a transducer that outputs the last n inputs.

    The returned transducer outputs the last n inputs if n < the number of
    inputs. If n >= the number of inputs, then outputs all inputs.

    No values will be outputted until the completion arity is called.

    Args:
        n: A number.
    """
    def xform(rf):
        buffer = _deque()

        def step(result, val):
            buffer.append(val)
            if len(buffer) > n:
                buffer.popleft()
            return result

        def complete(result):
            new_result = result
            while len(buffer) > 0:
                new_result = rf(new_result, buffer.popleft())
                if is_reduced(new_result):
                    buffer.clear()
            return rf(unreduced(new_result))

        return multi_arity(rf, complete, step)
    return xform


def take_nth(n):
    """Returns a transducer that outputs every nth input starting with the first.

    Args:
        n: A positive integer.
    """
    if n < 1 or n != int(n):
        raise ValueError('n must be a positive integer')
    return filter_indexed(lambda i, _: i % n == 0)


def take_while(pred):
    """Returns a transducer that outputs values until predicate returns false.

    Args:
        pred: A predicate function.
    """
    def xform(rf):
        @_step_safety
        def step(result, val):
            return rf(result, val) if pred(val) else reduced(result)
        return multi_arity(rf, rf, step)
    return xform


def drop(n):
    """Returns a transducer that drops the first n inputs.

    The returned transducer drops the first n inputs if n < the number of
    inputs. If n >= the number of inputs, then drops all inputs.

    Args:
        n: A number.
    """
    def xform(rf):
        remaining = n

        def step(result, val):
            nonlocal remaining
            remaining -= 1
            return result if remaining > -1 else rf(result, val)

        return multi_arity(rf, rf, step)
    return xform


def drop_last(n):
    """Returns a transducer that drops the last n values.

    The returned transducer drops the last n values if n < the number of
    inputs. If n >= the number of inputs, then drops all inputs.

    No values will be outputted until n inputs have been received.

    Args:
        n: A number.
    """
    def xform(rf):
        buffer = _deque()

        def step(result, val):
            buffer.append(val)
            if len(buffer) > n:
                return rf(result, buffer.popleft())
            return result

        def complete(result):
            buffer.clear()
            return rf(result)

        return multi_arity(rf, complete, step)
    return xform


def drop_while(pred):
    """Returns a transducer that drops values until predicate returns false.

    Args:
        pred: A predicate function.
    """
    def xform(rf):
        has_taken = False

        def step(result, val):
            nonlocal has_taken

            if not has_taken and pred(val):
                return result

            has_taken = True
            return rf(result, val)

        return multi_arity(rf, rf, step)
    return xform


def distinct(rf):
    """A transducer that removes duplicate values."""
    prev_vals = set()

    def step(result, val):
        if val in prev_vals:
            return result
        prev_vals.add(val)
        return rf(result, val)

    def complete(result):
        prev_vals.clear()
        return rf(result)

    return multi_arity(rf, complete, step)


def dedupe(rf):
    """A transducer that removes consecutive duplicate values."""
    prev_val = _Undefined

    def step(result, val):
        nonlocal prev_val
        if val == prev_val:
            return result
        prev_val = val
        return rf(result, val)

    return multi_arity(rf, rf, step)


def partition_all(n, step=None):
    """Returns a transducer that partitions all values.

    The returned transducer partitions values into tuples of size n that are
    step items apart. Partitions at the end may have a size < n.

    Args:
        n: An optional positive int representing the size of each partition
            (may be less for partitions at the end).
        step: An optional positive int used as the offset between partitions.
            If step < n, partitions will overlap n - step elements.
            If step == n, the default, no overlapping or dropping will occur.
            If step > n, step - n values will be dropped between partitions.

    See Also:
        partition
    """
    if step is None:
        step = n
    if n < 1 or n != int(n):
        raise ValueError('n must be a positive integer')
    if step < 1 or step != int(step):
        raise ValueError('step must be a positive integer')

    def xform(rf):
        buffer = []
        remaining_drops = 0

        def step_f(result, val):
            nonlocal buffer, remaining_drops

            if remaining_drops > 0:
                remaining_drops -= 1
                return result

            buffer.append(val)
            if len(buffer) < n:
                return result

            ret = rf(result, tuple(buffer))
            remaining_drops = max(0, step - n)
            buffer = [] if is_reduced(ret) else buffer[step:]
            return ret

        def complete(result):
            nonlocal buffer
            new_result = result

            while len(buffer) > 0:
                buf = tuple(buffer)
                buffer = buffer[step:]
                new_result = rf(new_result, buf)
                if is_reduced(new_result):
                    buffer.clear()

            return rf(unreduced(new_result))

        return multi_arity(rf, complete, step_f)
    return xform


def partition(n, step=None, pad=None):
    """Returns a transducer that partitions values into tuples of size n.

    The returned transducer partitions the values into tuples of size n
    that are step items apart.

    If the last partition size is greater than 0 but less than n:

    - If pad is None, the last partition is discarded.
    - If pad exists, its values will be used to fill the partition to a desired
      size of n. The padded partition will be outputted even if its size is < n.

    Args:
        n: A positive int representing the length of each partition. The last
            partition may be < n if pad is provided.
        step: An optional positive int used as the offset between partitions.
            If step < n, partitions will overlap n - step elements.
            If step == n, the default, no overlapping or dropping will occur.
            If step > n, step - n values will be dropped between partitions.
        pad: An optional iterable of any size. If the last partition size is
            greater than 0 and less than n, then pad will be applied to it.

    See Also:
        partition_all
    """
    def pad_xform(rf):
        def step_f(result, part):
            if len(part) == n:
                return rf(result, part)
            if pad is None:
                return reduced(result)
            padding = tuple(_itertools.islice(pad, n - len(part)))
            return ensure_reduced(rf(result, part + tuple(padding)))

        return multi_arity(rf, rf, step_f)
    return comp(partition_all(n, step), pad_xform)


def partition_by(f):
    """Returns a transducer that partitions inputs by f.

    In this context, a partition is defined as a tuple containing consecutive
    items for which f(item) returns the same value. That is to say, a new
    partition will be started each time f(item) returns a different value than
    the previous call.

    Args:
        f: A function accepting a single argument.
    """
    def xform(rf):
        prev_f_ret = _Undefined
        buffer = []

        def step(result, val):
            nonlocal prev_f_ret, buffer

            f_ret = f(val)
            if prev_f_ret is _Undefined or f_ret == prev_f_ret:
                prev_f_ret = f_ret
                buffer.append(val)
                return result

            prev_f_ret = f_ret
            rf_ret = rf(result, tuple(buffer))
            buffer = [] if is_reduced(rf_ret) else [val]
            return rf_ret

        def complete(result):
            if len(buffer) == 0:
                return rf(result)
            flushed_result = unreduced(rf(result, tuple(buffer)))
            buffer.clear()
            return rf(flushed_result)

        return multi_arity(rf, complete, step)
    return xform


def reductions(rf, init=_Undefined):
    """Returns a transducer that outputs each intermediate result from a reduction.

    The transformation first outputs init. From then on, all outputs will
    be of the form rf(prev_output, val) where val is an input to the
    transformation. rf will continue to get called until all input has been
    exhausted or rf returns a reduced value.

    Args:
        rf: A reducing function.
        init: An optional initial value.

    See Also:
        ireduce
    """
    if init is _Undefined:
        init = rf()

    def xform(xrf):
        prev_state = _Undefined

        def step(result, val):
            nonlocal prev_state

            if prev_state is _Undefined:
                prev_state = init
                result = xrf(result, init)
                if is_reduced(result):
                    return result

            prev_state = rf(prev_state, val)
            new_result = xrf(result, unreduced(prev_state))
            return (ensure_reduced(new_result)
                    if is_reduced(prev_state)
                    else new_result)

        def complete(result):
            if prev_state is _Undefined:
                tmp_result = unreduced(xrf(result, init))
            else:
                tmp_result = result
            return xrf(tmp_result)

        return multi_arity(xrf, complete, step)
    return xform


def interpose(sep):
    """Returns a transducer that outputs each input separated by sep."""
    def xform(rf):
        is_initial = True

        def step(result, val):
            nonlocal is_initial
            if is_initial:
                is_initial = False
                return rf(result, val)
            sep_result = rf(result, sep)
            return sep_result if is_reduced(sep_result) else rf(sep_result, val)

        return multi_arity(rf, rf, step)
    return xform


def replace(smap):
    """Returns a transducer that replaces values based on the given dictionary.

    The returned transducer replaces any input that's a key in smap with the
    key's corresponding value. Inputs that aren't a key in smap will be
    outputted without any transformation.

    Args:
        smap: A dictionary that maps values to their replacements.
    """
    def xform(rf):
        def step(result, val):
            return rf(result, smap.get(val, val))

        return multi_arity(rf, rf, step)
    return xform


def random_sample(prob):
    """Returns a transducer that selects inputs with the given probability.

    Args:
        prob: A number between 0 and 1.
    """
    return filter(lambda _: _random.random() < prob)
