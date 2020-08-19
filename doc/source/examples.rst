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

Working with Schemas
--------------------

By default tables will be written to the "public" schema. You can control this behavior however by specifying a ``tableauhyperapi.TableName`` when reading / writing extracts.

.. code-block:: python

   import pandas as pd
   import pantab
   from tableauhyperapi import TableName

   # Let's write somewhere besides the default public schema
   table = TableName("not_the_public_schema", "a_table")

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pantab.frame_to_hyper(df, "example.hyper", table=table)

   # Can also be round-tripped
   df2 = pantab.frame_from_hyper("example.hyper", table=table)

.. note::

   If you want to publish a hyper file using the Tableau Server REST API and you're using using a version prior to 2020.1 you'll need to have a single table named ``Extract`` that uses the ``Extract`` schema (``Extract.Extract``).


Reading and Writing Multiple Tables
-----------------------------------

``frames_to_hyper`` and ``frames_from_hyper`` can write and return a dictionary of DataFrames for Hyper extract, respectively.

.. code-block:: python

   import pandas as pd
   import pantab
   from tableauhyperapi import TableName

   dict_of_frames = {
       "table1": pd.DataFrame([[1, 2]], columns=list("ab")),
       TableName("non_public_schema", "table2"): pd.DataFrame([[3, 4]], columns=list("cd")),
   }

   pantab.frames_to_hyper(dict_of_frames, "example.hyper")

   # Can also be round-tripped
   result = pantab.frames_from_hyper("example.hyper")


.. note::

   While you can write using ``str``, ``tableauhyperapi.Name`` or ``tableauhyperapi.TableName`` instances, the keys of the dict returned by ``frames_from_hyper`` will always be ``tableauhyperapi.TableName`` instances

Appending Data to Existing Tables
---------------------------------

By default, ``frame_to_hyper`` and ``frames_to_hyper`` will fully drop and reloaded targeted tables. However, you can also append records to existing tables by supplying ``table_mode="a"`` as a keyword argument.

.. code-block:: python

   import pandas as pd
   import pantab

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   pantab.frame_to_hyper(df, "example.hyper", table="animals")

   new_data = pd.DataFrame([["moose", 4]], columns=["animal", "num_of_legs"])

   # Instead of overwriting the animals table, we can append via table_mode
   pantab.frame_to_hyper(df, "example.hyper", table="animals", table_mode="a")

Please note that ``table_mode="a"`` will create the table(s) if they do not already exist.
