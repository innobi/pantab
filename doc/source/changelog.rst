Changelog
^^^^^^^^^

0.0.1.b3 (2019-11-01)
=====================

Features
--------

- Added support for nullable integer types (i.e. the "Int*" types in pandas). Current integer types will now show as NOT_NULLABLE in Hyper extracts. (#7)
- Added support for reading / writing UTC timestamps, rather than only timezone-naive. (#8)


Bugfixes
--------

- Fixed issue where certain versions of pantab in combination with certain versions of the Hyper API would throw "TypeError: __init__() got an unexpected keyword argument 'name'" when generating Hyper extracts. (#10)
