from functools import reduce
from toolz import pipe, concat
from math import inf


def multiply(*vals):
    return reduce(lambda x, y: x * y, vals)


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
