Usage Examples
==============

Writing to a Hyper Extract
--------------------------

.. code-block:: python

   import pandas as pd
   import pantab

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pantab.frame_to_hyper(df, "example.hyper", table="animals")

The above example will write out to a file named "example.hyper", which Tableau can then report off of.

.. image:: _static/simple_write.png

Reading a Hyper Extract
-----------------------

.. code-block:: python

   import pantab

   df = pantab.frame_from_hyper("example.hyper", table="animals")
   print(df)

Specifying a Schema
-------------------

.. code-block:: python

   import pandas as pd
   import pantab

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pantab.frame_to_hyper(df, "example.hyper", schema="a_schema", table="animals")

   # Can also be round-tripped
   df2 = pantab.frame_from_hyper(df, "example.hyper", schema="a_schema", table="animals")

.. todo::

   Support for reading / writing multiple tables
