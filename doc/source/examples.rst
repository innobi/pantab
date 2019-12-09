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
   df2 = pantab.frame_from_hyper(df, "example.hyper", table=table)

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

Inserting New Data into Existing Tables
---------------------------------------

In addition to append, ``table_mode="i"`` can be used to combine a df with an existing table, only inserting new records. A unique key ``table_key="column"`` must be supplied when using this method.

.. code-block:: python

   import pandas as pd
   import pantab

   df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   # Writing out first df
   pantab.frame_to_hyper(df, "example.hyper", table="animals")

   updated_df = pd.DataFrame([
       ["dog", 4],
       ["cat", 4],
       ["snake", 0],
   ], columns=["animal", "num_of_legs"])

   # In this case, only snake will be inserted into your existing table.
   pantab.frame_to_hyper(updated_df, "example.hyper", table="animals", table_mode="i", table_key="animal")

If you're using ``frames_to_hyper`` to update multiple tables, use the arg ``list_of_keys=["column_x", "column_y"]`` to specify a key for each frame (in order of the frames supplied).

.. code-block:: python

   import pandas as pd
   import pantab

   df_animals = pd.DataFrame([
      ["dog", 4],
      ["cat", 4],
   ], columns=["animal", "num_of_legs"])

   df_places = pd.DataFrame([
      ["London", 1],
      ["Paris", 2],
      ["New York", 3],
   ], columns=["place", "ranking"])

   # Create .hyper extract.
   pantab.frames_to_hyper(dict_of_frames={"animals" : df_animals, "places" : df_places}, database="example.hyper")

   upd_animals = pd.DataFrame([
      ["dog", 4],
      ["cat", 4],
      ["snake", 0],
      ["kangaroo", 2],
   ], columns=["animal", "num_of_legs"])

   upd_places = pd.DataFrame([
      ["London", 1],
      ["Paris", 2],
      ["New York", 3],
      ["Tokyo", 4],
   ], columns=["place", "ranking"])

   # Insert new values into existing .hyper tables.
   pantab.frames_to_hyper(
        dict_of_frames={"animals" : upd_animals, "places" : upd_places},
        list_of_keys=["animal", "place"],
        database="example.hyper",
        table_mode="i",
   )

..  note::

   Unlike append, ``table_mode="i"`` requires the table(s) to exist before new records can be inserted.