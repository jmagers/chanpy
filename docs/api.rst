API
===

Channels, the center around any CSP library. The :any:`core` module provides
all the essential functions for creating and managing them. For convenience,
all of the public members of :any:`core` exist at the top-level of the package.

Like Clojure's core.async, ChanPy channels have direct support for
transformations via transducers. The :any:`transducers` module provides many
transducers as well as functions to help create and use them.


Core
----

.. automodule:: chanpy.core
   :members:
   :imported-members:


Transducers
-----------

.. automodule:: chanpy.transducers
   :members:
