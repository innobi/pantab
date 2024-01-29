Changelog
^^^^^^^^^

Pantab 4.0.0 (XXXX-XX-XX)
=========================

pantab 4.0 represents the most significant change to the library since its 5 years ago. Please note 4.0 introduces *breaking changes* to the API. When in doubt, users should pin pantab to the 3.x series in production and test before upgrading.

New Features
------------

Support for pandas, pyarrow, polars and more!
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original design of pantab was heavily tied to the internals of pandas. Unfortunately, the type system pandas inherited from NumPy was not an ideal match for translating into Hyper types. Since that time, the `Arrow Columnar Format <https://arrow.apache.org/docs/format/Columnar.html>`_ has helped immensely to standardize the way libraries could efficiently exchange data. As a result, pantab can support exchanging information from pandas, pyarrow and polars dataframes with relative ease.

All of the following solutions will work:

.. code-block:: python

   >>> import pantab as pt

   >>> import pandas as pd
   >>> df = pd.DataFrame({"col": [1, 2, 3]})
   >>> pt.frame_to_hyper(df, "example.hyper", table="test")

   >>> import pyarrow as pa
   >>> tbl = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["col"])
   >>> pt.frame_to_hyper(tbl, "example.hyper", table="test")

   >>> import polars as pl
   >>> df = pl.DataFrame({"col": [1, 2, 3]})
   >>> pt.frame_to_hyper(df, "example.hyper", table="test")


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

   Any library that implements the `Arrow PyCapsule Interface <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>_` will be *writeable* via pantab; reading to such a library would require explicit development

Read any Hyper file
~~~~~~~~~~~~~~~~~~~

Prior to the 4.0 release, pantab worked well as a "self-contained" system, i.e. it could roundtrip files that it itself created. However, pantab struggled to read in hyper files created from other sources given. With 4.0, pantab makes a promise to be able to read *any* hyper file regardless of the types therein.


Native Date/Time Support
~~~~~~~~~~~~~~~~~~~~~~~~

pandas historically only had a timestamp type with nanosecond precision from the Unix epoch. Thanks to the arrow type system, users can now write dates and even times

.. code-block:: python

   >>> import pantab as pt
   >>> import pyarrow as pa
   >>> tbl = pa.Table.from_arrays([pa.array([datetime.date(2024, 1, 1)])], names=["col"])
   >>> pt.frame_to_hyper(tbl, "example.hyper", table="test")  # this will now write dates!

Write JSON / Geography
~~~~~~~~~~~~~~~~~~~~~~

Arrow does not have a native JSON string type nor a geography type. To work around this, you may still pass in either type as a string and use the ``json_columns`` and ``geo_columns`` arguments respectively, providing a ``set`` of column names that are applicable. pantab takes care of the rest!

.. code-block:: python

   >>> import pantab as pt
   >>> import pandas as pd
   >>> df = pd.DataFrame({"json_col": ['{"foo": 42}']})
   >>> pt.frame_to_hyper(df, "example.hyper", table="test", json_columns={"json_col"})

   >>> import polars as pl
   >>> df = pl.DataFrame({"geo_col": ["point(-122.338083 47.647528)"]})
   >>> pt.frame_to_hyper(df, "example.hyper", table="test", geo_columns={"geo_col"})

.. note::

   The Hyper API reads back geography types as a binary proprietary format. You can still _write_ this back via pantab, but note that you can not roundtrip a WKT like the above example

Better Performance
~~~~~~~~~~~~~~~~~~

Reading in particular has much improved performance thanks to the new design. Compared to pantab 3.X, reads in pantab 4.0 are *at least* 5x faster and use only 20% of the memory

Miscellaneous
~~~~~~~~~~~~~

* By default all columns written via pantab are assumed to be nullable. You can override this behavior by passing a set of column names to the ``not_null_columns`` argument when writing
* pantab will now handle duplicate column names during reads by appending ``_n`` to every duplicate, where n represents the 0-based counter of a given column name's occurrance

Backwards incompatible changes
------------------------------

* The ability to provide your own existing Hyper connection to pantab has been removed. This was removed due to the perceived incompatability between the 3.X and 4.X designs, and the development effort would be rather large for what is believed to be a seldomly used feature

Bug Fixes
---------

* Fixed a segmentation fault when writing certain frames (#240)
* Fixed a memory error when writing empty frames (#172)


Pantab 3.0.3 (2023-12-18)
=========================

- Fixed issue with reading VARCHAR columns from Hyper files (#210)

Pantab 3.0.2 (2023-11-13)
=========================

- Fixed issue with NumPy throwing ``RuntimeError: module compiled against API version 0x10 but this version of numpy is 0xe``

Pantab 3.0.1 (2023-10-09)
=========================
Special thanks to `Abhinav Dhulipala <https://github.com/abhinavDhulipala>`_ for contributing to  this release.

- Fixed issue where timezone-aware datetimes with pandas >= 1.4 would write incorrect values to Hyper (#186)
- Fixed issue where a query returning an empty result set from Hyper would raise ``ValueError`` (#163)


Pantab 3.0.0 (2022-09-14)
=========================

- Implemented a new ``use_parquet`` keyword in ``frame_to_hyper`` which uses Parquet as an intermediate storage solution instead of pantab's own internal C library. This may provide a small performance boost at the cost of additional disk usage
- Fixed issue where pantab was not compatabile with Hyper versions 0.0.14567 and above. See the :ref:`compatability` documentation.


Pantab 2.1.1 (2022-04-13)
=========================

- Fixed a memory leak with ``frame_to_hyper``
- Fixed issue where ``pantab.__version__`` was misreporting the version string

Pantab 2.1.0 (2021-07-02)
=========================
Special thanks to `Caleb Overman <https://github.com/caleboverman>`_ for contributing to  this release.

Enhancments
-----------

- A new ``use_float_na`` parameter has been added to reading functions, which will convert doubles from Hyper files to the pandas ``Float64`` Extension dtype rather than using the standard numpy float dtype (#131)
- Writing ``Float32`` and ``Float64`` dtypes is now supported (#131)
- Writing to a Hyper file  is now up to 50% faster (#132)

Pantab 2.0.0 (2021-04-15)
=========================

Special thanks to `Adrian Vogelsgesang <https://github.com/vogelsgesang>`_ for contributing to this release.

API Breaking Changes
--------------------

- Users may now pass an existing connection as the first argument to pantab's read functions. As part of this, the first argument was renamed from ``database`` to ``source`` (#123)

Enhancements
------------

- Added support for Python 3.9 while dropping support for 3.6 (#122)
- A new ``frame_from_hyper_query`` method has been added, providing support for executing SQL statements against a Hyper file (#118)
- Users may now create their own Hyper process and pass it as an argument to the reading and writing functions (#39, #51)
- The value 0001-01-01 will no longer be read as a NULL timestamp (#121)


Pantab 1.1.1 (2020-11-02)
=========================

Bugfixes
--------

- Fixed issue where pantab would throw ``TypeError: Column "COLUMN_NAME" has unsupported datatype TEXT`` when reading Non-Nullable string columns from Hyper (#111)


Pantab 1.1.0 (2020-04-30)
=========================

Special thanks to `Adrian Vogelsgesang <https://github.com/vogelsgesang>`_ for contributing to this release.

Features
--------

- Added support for reading Hyper DATE columns as datetime64 objects in pandas (#94)


Bugfixes
--------

- Fixed issue where Python would crash instead of throwing an error when reading invalid records from a Hyper file (#77)
- Fixed ImportError when building from source with tableauhyperapi versions 0.0.10309 and greater (#88)
- Attempting to read a Hyper extract with unsupported data types will now raise a ``TypeError`` (#92)


Pantab 1.0.1 (2020-02-03)
=========================

Features
--------

- pantab will not automatically install the tableauhyperapi as a dependency when installing via pip (#83)
- Pre-built wheels for manylinux configurations are now available. (#84)


Pantab 1.0.0 (2020-01-15)
=========================

Special thanks to `chillerno1 <https://github.com/chillerno1>`_ for contributing to this release.

Features
--------

- pantab now supports reading/writing pandas 1.0 dtypes, namely the ``boolean`` and ``string`` dtypes. (#20)

  .. important::

     TEXT data read from a Hyper extract will be stored in a ``string`` dtype when using pandas 1.0 or greater in combination with pantab 1.0 or greater. Older versions of either tool will read the data back into a ``object`` dtype.


Bugfixes
--------

- Fixed potential segfault on systems where not all addresses can be expressed in an unsigned long long. (#52)


Pantab 0.2.3 (2020-01-02)
=========================

Bugfixes
--------

- Fixed issue where dates would roundtrip in pantab find but would either error or be incorrect in Tableau Desktop (#66)


Pantab 0.2.2 (2019-12-25)
=========================

Bugfixes
--------

- Pantab now writes actual NULL values for datetime columns, rather than 0001-01-01 00:00:00 (#60)


Pantab 0.2.1 (2019-12-23)
=========================

Bugfixes
--------

- Fixed issue where reading a datetime column containing ``pd.NaT`` values would throw an ``OutOfBoundsDatetime`` error (#56)
- Fixed issue where reading a timedelta column containing ``pd.NaT`` would throw a ``ValueError`` (#57)


Pantab 0.2.0 (2019-12-19)
=========================

Features
--------

- Improved performance when reading data from Hyper extracts (#34)


0.1.1 (2019-12-06)
==================

A special *thank you* goes out to the following contributors leading up to this release:

  - `chillerno1 <https://github.com/chillerno1>`_
  - `cedricyau <https://github.com/cedricyau>`_

Bugfixes
--------

- Fixed issue where source installations would error with `fatal error: tableauhyperapi.h: No such file or directory` (#40)


0.1.0 (2019-11-29)
==================
*pantab is officially out of beta!* Thanks for all of the feedback and support of the tool so far.

Special thanks to Adrian Vogelsgesang and Jan Finis at Tableau, who offered guidance and feedback on performance improvements in this release.

- Improved error messaging when attempting to write invalid data. (#19)
- Write-performance of Hyper extracts has been drastically improved for larger datasets. (#31)
- Less memory is now required to write DataFrames to the Hyper format. (#33)


0.0.1.b5 (2019-11-05)
=====================

Bugfixes
--------

- Fixed issue where failures during append mode (``table_mode="a"``) would delete original Hyper file. (#17)


0.0.1.b4 (2019-11-05)
=====================

Features
--------

- frame_to_hyper and frames_to_hyper now support a table_mode keyword argument. ``table_mode="a"`` will append data to existing tables, or create them if they do not exist. The default operation of ``table_mode="w"`` will continue to fully drop / reload tables. (#14)


0.0.1.b3 (2019-11-01)
=====================

Features
--------

- Added support for nullable integer types (i.e. the "Int*" types in pandas). Current integer types will now show as NOT_NULLABLE in Hyper extracts. (#7)
- Added support for reading / writing UTC timestamps, rather than only timezone-naive. (#8)


Bugfixes
--------

- Fixed issue where certain versions of pantab in combination with certain versions of the Hyper API would throw "TypeError: __init__() got an unexpected keyword argument 'name'" when generating Hyper extracts. (#10)
