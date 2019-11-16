from genericfuncs import (multiArity, reduce, ensureReduced, unreduced,
                          isReduced, Reduced)
from toolz import identity, partial as prt, compose as comp
from collections import deque


def _transduce(xform, rf, init, coll):
    xrf = xform(rf)
    return xrf(reduce(xrf, init, coll))


transduce = multiArity(None, None, None,
                       lambda xf, rf, coll: _transduce(xf, rf, rf(), coll),
                       _transduce)


def sequence(xf, coll):
    def flushBuffer(buf):
        b = unreduced(buf)
        while len(b) != 0:
            yield b.popleft()

    rf = xf(multiArity(None,
                       identity,
                       lambda result, val: result.append(val) or result))
    buffer = deque()
    for x in coll:
        buffer = rf(buffer, x)
        yield from flushBuffer(buffer)
        if isReduced(buffer):
            break
    yield from rf(unreduced(buffer))


def map(f):
    return lambda rf: multiArity(rf, rf,
                                 lambda result, val: rf(result, f(val)))


def filter(pred):
    return lambda rf: multiArity(rf, rf,
                                 lambda result, val: (rf(result, val)
                                                      if pred(val)
                                                      else result))


def cat(rf):
    return multiArity(rf, rf, prt(reduce, rf))


def mapcat(f):
    return comp(map(f), cat)


def take(n):
    def xform(rf):
        remaining = n

        def step(result, val):
            nonlocal remaining
            newResult = rf(result, val) if remaining > 0 else result
            remaining -= 1
            return ensureReduced(newResult) if remaining <= 0 else newResult

        return multiArity(rf, rf, step)
    return xform


def takeWhile(f):
    def xform(rf):
        return multiArity(rf, rf, lambda result, val: (rf(result, val)
                                                       if f(val)
                                                       else Reduced(result)))
    return xform


def partitionAll(n):
    def xform(rf):
        buffer = []

        def step(result, val):
            nonlocal buffer
            buffer.append(val)
            if len(buffer) < n:
                return result
            newResult = rf(result, tuple(buffer))
            buffer.clear()
            return newResult

        def complete(result):
            if len(buffer) == 0:
                return rf(result)
            flushedResult = unreduced(rf(result, tuple(buffer)))
            return rf(flushedResult)

        return multiArity(rf, complete, step)
    return xform


def _reductions(f, init):
    class _INITIAL:
        pass

    def xform(rf):
        prevState = _INITIAL

        def step(result, val):
            nonlocal prevState

            if prevState is _INITIAL:
                prevState = init
                result = rf(result, init)
                if isReduced(result):
                    return result

            prevState = f(prevState, val)
            newResult = rf(result, unreduced(prevState))
            return (ensureReduced(newResult)
                    if isReduced(prevState)
                    else newResult)

        return multiArity(rf, rf, step)
    return xform


reductions = multiArity(None, lambda f: _reductions(f, f()), _reductions)
