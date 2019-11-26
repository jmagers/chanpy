import functools
from collections import deque


def comp(*funcs):
    ordered_funcs = reversed(funcs)
    try:
        first_func = next(reversed(funcs))
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


def take_while(f):
    def xform(rf):
        return multi_arity(rf, rf, lambda result, val: (rf(result, val)
                                                        if f(val)
                                                        else reduced(result)))
    return xform


def partition_all(n):
    def xform(rf):
        buffer = []

        def step(result, val):
            nonlocal buffer
            buffer.append(val)
            if len(buffer) < n:
                return result
            new_result = rf(result, tuple(buffer))
            buffer.clear()
            return new_result

        def complete(result):
            if len(buffer) == 0:
                return rf(result)
            flushedResult = unreduced(rf(result, tuple(buffer)))
            return rf(flushedResult)

        return multi_arity(rf, complete, step)
    return xform


def reductions(f, init):
    class _INITIAL:
        pass

    def xform(rf):
        prev_state = _INITIAL

        def step(result, val):
            nonlocal prev_state

            if prev_state is _INITIAL:
                prev_state = init
                result = rf(result, init)
                if is_reduced(result):
                    return result

            prev_state = f(prev_state, val)
            new_result = rf(result, unreduced(prev_state))
            return (ensure_reduced(new_result)
                    if is_reduced(prev_state)
                    else new_result)

        return multi_arity(rf, rf, step)
    return xform
