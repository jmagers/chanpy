# ChanPy

A CSP Python library based on Clojure core.async. ChanPy implements
equivalents for all the functions in
[core.async](https://clojure.github.io/core.async/) and provides channels that
can be used with or without asyncio. ChanPy even provides support for applying
transformations across channels in the same way Clojure does, via
[transducers](https://clojure.org/reference/transducers).

## Getting Started

### Documentation

Documentation is hosted at https://chanpy.readthedocs.io/

### Prerequisites

Python 3.7 or greater.

### Installation

Available on PyPI:

```shell
pip3 install chanpy
```

## License

Apache License 2.0

## Inspiration

* Clojure [core.async](https://github.com/clojure/core.async/) for providing
  one of the best CSP designs of all time.
* [aiochan](https://github.com/zh217/aiochan) for proving how well asyncio can
  be used with Clojure flavored CSP.
