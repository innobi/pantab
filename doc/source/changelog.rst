Changelog
^^^^^^^^^

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
