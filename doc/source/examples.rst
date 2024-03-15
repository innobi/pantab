Usage Examples
==============

Writing to a Hyper Extract
--------------------------

.. code-block:: python

   import pandas as pd
   import pantab as pt

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pt.frame_to_hyper(df, "example.hyper", table="animals")

The above example will write out to a file named "example.hyper", which Tableau can then report off of.

.. image:: _static/simple_write.png

Reading a Hyper Extract
-----------------------

.. code-block:: python

   import pantab as pt

   df = pt.frame_from_hyper("example.hyper", table="animals")
   print(df)

Working with Schemas
--------------------

By default tables will be written to the "public" schema. You can control this behavior however by specifying a ``tableauhyperapi.TableName`` when reading / writing extracts.

.. code-block:: python

   import pandas as pd
   import pantab as pt
   from tableauhyperapi import TableName

   # Let's write somewhere besides the default public schema
   table = TableName("not_the_public_schema", "a_table")

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pt.frame_to_hyper(df, "example.hyper", table=table)

   # Can also be round-tripped
   df2 = pt.frame_from_hyper("example.hyper", table=table)

.. note::

   If you want to publish a hyper file using the Tableau Server REST API and you're using using a version prior to 2020.1 you'll need to have a single table named ``Extract`` that uses the ``Extract`` schema (``Extract.Extract``).


Reading and Writing Multiple Tables
-----------------------------------

``frames_to_hyper`` and ``frames_from_hyper`` can write and return a dictionary of DataFrames for Hyper extract, respectively.

.. code-block:: python

   import pandas as pd
   import pantab as pt
   from tableauhyperapi import TableName

   dict_of_frames = {
       "table1": pd.DataFrame([[1, 2]], columns=list("ab")),
       TableName("non_public_schema", "table2"): pd.DataFrame([[3, 4]], columns=list("cd")),
   }

   pt.frames_to_hyper(dict_of_frames, "example.hyper")

   # Can also be round-tripped
   result = pt.frames_from_hyper("example.hyper")


.. note::

   While you can write using ``str``, ``tableauhyperapi.Name`` or ``tableauhyperapi.TableName`` instances, the keys of the dict returned by ``frames_from_hyper`` will always be ``tableauhyperapi.TableName`` instances

Appending Data to Existing Tables
---------------------------------

By default, ``frame_to_hyper`` and ``frames_to_hyper`` will fully drop and reloaded targeted tables. However, you can also append records to existing tables by supplying ``table_mode="a"`` as a keyword argument.

.. code-block:: python

   import pandas as pd
   import pantab as pt

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pt.frame_to_hyper(df, "example.hyper", table="animals")

   new_data = pd.DataFrame([["moose", 4]], columns=["animal", "num_of_legs"])

   # Instead of overwriting the animals table, we can append via table_mode
   pt.frame_to_hyper(df, "example.hyper", table="animals", table_mode="a")

Please note that ``table_mode="a"`` will create the table(s) if they do not already exist.


Issuing SQL queries
-------------------

With ``frame_from_hyper_query``, one can execute SQL queries against a Hyper file and retrieve the resulting data as a DataFrame. This can be used, e.g. to retrieve only a part of the data (using a ``WHERE`` clause) or to offload computations to Hyper.

.. code-block:: python

   import pandas as pd
   import pantab as pt

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
       ["moose", 4],
       ["centipede", 100],
   ], columns=["animal", "num_of_legs"])

   pt.frame_to_hyper(df, "example.hyper", table="animals")

   # Read a subset of the data from the Hyper file
   query = """
   SELECT animal
   FROM animals
   WHERE num_of_legs > 4
   """
   df = pt.frame_from_hyper_query("example.hyper", query)
   print(df)

   # Let Hyper do an aggregation for us - it could also do joins, window queries, ...
   query = """
   SELECT num_of_legs, COUNT(*)
   FROM animals
   GROUP BY num_of_legs
   """
   df = pt.frame_from_hyper_query("example.hyper", query)
   print(df)


Bring your own DataFrame
------------------------

.. versionadded:: 4.0

When pantab was first created, pandas was the dominant DataFrame library. In the years since then, many competing libraries have cropped up which all provide different advantages. To give users the most flexibility, pantab provides first class support for exchanging `pandas <https://pandas.pydata.org/>`_, `polars <https://pola.rs/>`_ and `pyarrow <https://arrow.apache.org/docs/python/index.html>`_ DataFrames. To wit, all of the following code samples will produce an equivalent Hyper file:

.. code-block:: python

   import pantab as pt

   import pandas as pd
   df = pd.DataFrame({"col": [1, 2, 3]})
   pt.frame_to_hyper(df, "example.hyper", table="test")

   import pyarrow as pa
   tbl = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["col"])
   pt.frame_to_hyper(tbl, "example.hyper", table="test")

   import polars as pl
   df = pl.DataFrame({"col": [1, 2, 3]})
   pt.frame_to_hyper(df, "example.hyper", table="test")

As far as reading is concerned, you can control the type of DataFrame you receive back via the ``return_type`` keyword. pandas remains the default

.. code-block:: python

   >>> pt.frame_from_hyper("example.hyper", table="test")  # pandas by default
      col
   0    1
   1    2
   2    3
   >>> pt.frame_from_hyper("example.hyper", table="test", return_type="pyarrow")
   pyarrow.Table
   col: int64
   ----
   col: [[1,2,3]]
   >>> pt.frame_from_hyper("example.hyper", table="test", return_type="polars")
   shape: (3, 1)
   ┌─────┐
   │ col │
   │ --- │
   │ i64 │
   ╞═════╡
   │ 1   │
   │ 2   │
   │ 3   │
   └─────┘

.. note::

   Technically pantab is able to *write* any DataFrame library that implements the `Arrow PyCapsule Interface <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>`_
