Pantab 0.0.1.b4 (2019-11-05)
============================

Features
--------

- Added support for nullable integer types (i.e. the "Int*" types in pandas). Current integer types will now show as NOT_NULLABLE in Hyper extracts. (#7)
- Added support for reading / writing UTC timestamps, rather than only timezone-naive. (#8)
- frame_to_hyper and frames_to_hyper now support a table_mode keyword argument. ``table_mode="a"`` will append data to existing tables, or create them if they do not exist. The default operation of ``table_mode="w"`` will continue to fully drop / reload tables. (#14)


Bugfixes
--------

- Fixed issue where certain versions of pantab in combination with certain versions of the Hyper API would throw "TypeError: __init__() got an unexpected keyword argument 'name'" when generating Hyper extracts. (#10)
