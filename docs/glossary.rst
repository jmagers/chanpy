Glossary
========

.. glossary::

   reducing function
      A type of function used for reduction.

      It may have up to three different arities:

      * The *step* arity accepts 2 arguments, the accumulated result and an
        input. It returns the new accumulated result of the reduction.
      * The *init* arity is optional and accepts 0 arguments. It returns an
        initial value for the accumulated result if one is not explicitly
        provided.
      * The *completion* arity is required only when used with a
        :any:`transducer` and accepts 1 argument, the accumulated result of
        the reduction. See
        `clojure.org <https://clojure.org/reference/transducers>`_ for more
        information about use with transducers.

      :any:`multi_arity()` can be used to help create these multi-arity
      functions.

      Reducing functions additionally support a form of early termination via
      :any:`reduced` values.


   transducer
      Also known as a :any:`reducing function` transformer, it's simply a
      function that accepts a reducing function as input and returns a new
      reducing function as output. It's commonly referred to as a
      transformation or xform throughout the documentation.

      The :any:`transducers` module provides many transducers as well as
      functions to help create and use them.

      See `clojure.org <https://clojure.org/reference/transducers>`_ for
      information about transducers.
