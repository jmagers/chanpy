import functools
from toolz import pipe, concat
from math import inf


def multiArity(*funcs):
    def dispatch(*args):
        try:
            func = funcs[len(args)]
            if func is None:
                raise IndexError
        except IndexError:
            raise ValueError('wrong number of arguments supplied')
        return func(*args)
    return dispatch


class Reduced:
    def __init__(self, value):
        self.value = value


def isReduced(value):
    return isinstance(value, Reduced)


def ensureReduced(x):
    return x if isReduced(x) else Reduced(x)


def unreduced(x):
    return x.value if isReduced(x) else x


def _reduce(rf, init, coll):
    result = init
    for x in coll:
        if isReduced(result):
            return result.value
        result = rf(result, x)
    return result


reduce = multiArity(None,
                    None,
                    lambda rf, coll: _reduce(rf, rf(), coll),
                    _reduce)


def multiply(*vals):
    return functools.reduce(lambda x, y: x * y, vals)


def repeat(value, n=inf):
    i = 0
    while i < n:
        yield value
        i += 1


def cycle(x):
    return concat(repeat(x))


def tup(*args):
    return tuple(args)


def reductions(func, init, seq):
    """Like 'reductions' in clojure but requires initial value"""
    yield init
    prev = init
    for value in seq:
        prev = func(prev, value)
        yield prev


def flow(firstFunc, *funcs):
    def newFunc(*args, **kw):
        return pipe(firstFunc(*args, **kw),
                    *funcs)
    return newFunc


def run(func, *seqs):
    for vals in zip(*seqs):
        func(*vals)


def iterToMap(iterable):
    return dict(enumerate(iterable))


def constantly(x):
    return lambda *args, **kw: x


def dictToFunc(dictionary):
    return lambda x: dictionary[x]


def applyArgs(func, *args):
    """Similar to Clojure apply"""
    argsList = list(args)
    return func(*argsList[:-1], *argsList[-1])


def mergeWith(f, *dicts):
    newDict = {}
    for d in dicts:
        for key, val in d.items():
            if key in newDict:
                newDict[key] = f(newDict[key], val)
            else:
                newDict[key] = val
    return newDict


def isNonNegative(x):
    try:
        return x >= 0
    except TypeError:
        return False
