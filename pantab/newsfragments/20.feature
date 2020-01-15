pantab now supports reading/writing pandas 1.0 dtypes, namely the ``boolean`` and ``string`` dtypes.

.. important::

   TEXT data read from a Hyper extract will be stored in a ``"string"`` dtype when using pandas 1.0 or greater in combination with pantab 1.0 or greater. Older versions of either tool will read the data back into a ``object`` dtype.