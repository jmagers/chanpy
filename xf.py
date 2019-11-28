import functools
from collections import deque


class _UNDEFINED:
    pass


def comp(*funcs):
    ordered_funcs = reversed(funcs)
    try:
        first_func = next(ordered_funcs)
    except StopIteration:
        return identity
    ordered_funcs = tuple(ordered_funcs)

    def wrapper(*args, **kwargs):
        result = first_func(*args, **kwargs)
        for f in ordered_funcs:
            result = f(result)
        return result

    return wrapper


def multi_arity(*funcs):
    def dispatch(*args):
        try:
            func = funcs[len(args)]
            if func is None:
                raise IndexError
        except IndexError:
            raise ValueError('wrong number of arguments supplied')
        return func(*args)
    return dispatch


class _Reduced:
    def __init__(self, value):
        self.value = value


def reduced(value):
    return _Reduced(value)


def is_reduced(value):
    return isinstance(value, _Reduced)


def ensure_reduced(x):
    return x if is_reduced(x) else reduced(x)


def unreduced(x):
    return x.value if is_reduced(x) else x


def ireduce(rf, init, coll):
    result = init
    for x in coll:
        result = rf(result, x)
        if is_reduced(result):
            return unreduced(result)
    return result


def itransduce(xform, rf, init, coll):
    xrf = xform(rf)
    return xrf(ireduce(xrf, init, coll))


def identity(x):
    return x


def xiter(xform, coll):
    def flush_buffer(buf):
        b = unreduced(buf)
        while len(b) != 0:
            yield b.popleft()

    rf = xform(multi_arity(None,
                           identity,
                           lambda result, val: result.append(val) or result))
    buffer = deque()
    for x in coll:
        buffer = rf(buffer, x)
        yield from flush_buffer(buffer)
        if is_reduced(buffer):
            break
    yield from rf(unreduced(buffer))


def map(f):
    return lambda rf: multi_arity(rf, rf,
                                  lambda result, val: rf(result, f(val)))


def filter(pred):
    return lambda rf: multi_arity(rf, rf,
                                  lambda result, val: (rf(result, val)
                                                       if pred(val)
                                                       else result))


def remove(pred):
    return filter(lambda x: not pred(x))


def keep(f):
    return comp(map(f), filter(lambda x: x is not None))


def cat(rf):
    return multi_arity(rf, rf, functools.partial(ireduce, rf))


def mapcat(f):
    return comp(map(f), cat)


def take(n):
    def xform(rf):
        remaining = n

        def step(result, val):
            nonlocal remaining
            new_result = rf(result, val) if remaining > 0 else result
            remaining -= 1
            return ensure_reduced(new_result) if remaining <= 0 else new_result

        return multi_arity(rf, rf, step)
    return xform


def take_nth(n):
    if n < 1 or n != int(n):
        raise ValueError("n must be a nonnegative integer")

    def xform(rf):
        count = n

        def step(result, val):
            nonlocal count
            if count >= n:
                count = 1
                return rf(result, val)
            count += 1
            return result

        return multi_arity(rf, rf, step)
    return xform


def take_while(pred):
    def xform(rf):
        return multi_arity(rf, rf, lambda result, val: (rf(result, val)
                                                        if pred(val)
                                                        else reduced(result)))
    return xform


def drop(n):
    def xform(rf):
        remaining = n

        def step(result, val):
            nonlocal remaining
            remaining -= 1
            return result if remaining > -1 else rf(result, val)

        return multi_arity(rf, rf, step)
    return xform


def drop_while(pred):
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
    prev_val = _UNDEFINED

    def step(result, val):
        nonlocal prev_val
        if val == prev_val:
            return result
        prev_val = val
        return rf(result, val)

    return multi_arity(rf, rf, step)


def partition_all(n):
    if n < 1 or n != int(n):
        raise ValueError("n must be a nonnegative integer")

    def xform(rf):
        buffer = []

        def step(result, val):
            nonlocal buffer
            buffer.append(val)
            if len(buffer) < n:
                return result
            buf = tuple(buffer)
            buffer.clear()
            return rf(result, buf)

        def complete(result):
            if len(buffer) == 0:
                return rf(result)
            flushed_result = unreduced(rf(result, tuple(buffer)))
            buffer.clear()
            return rf(flushed_result)

        return multi_arity(rf, complete, step)
    return xform


def partition_by(f):
    def xform(rf):
        prev_ret = _UNDEFINED
        buffer = []

        def step(result, val):
            nonlocal prev_ret, buffer

            ret = f(val)
            if prev_ret is _UNDEFINED or ret == prev_ret:
                prev_ret = ret
                buffer.append(val)
                return result

            prev_ret = ret
            buf = tuple(buffer)
            buffer = [val]
            return rf(result, buf)

        def complete(result):
            if len(buffer) == 0:
                return rf(result)
            flushed_result = unreduced(rf(result, tuple(buffer)))
            buffer.clear()
            return rf(flushed_result)

        return multi_arity(rf, complete, step)
    return xform


def reductions(f, init):
    def xform(rf):
        prev_state = _UNDEFINED

        def step(result, val):
            nonlocal prev_state

            if prev_state is _UNDEFINED:
                prev_state = init
                result = rf(result, init)
                if is_reduced(result):
                    return result

            prev_state = f(prev_state, val)
            new_result = rf(result, unreduced(prev_state))
            return (ensure_reduced(new_result)
                    if is_reduced(prev_state)
                    else new_result)

        def complete(result):
            if prev_state is _UNDEFINED:
                tmp_result = unreduced(rf(result, init))
            else:
                tmp_result = result
            return rf(tmp_result)

        return multi_arity(rf, complete, step)
    return xform


def replace(smap):
    def xform(rf):
        def step(result, val):
            return rf(result, smap.get(val, val))

        return multi_arity(rf, rf, step)
    return xform
