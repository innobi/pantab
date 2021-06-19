import collections
from typing import Union

import tableauhyperapi as tab_api

import pantab._compat as compat

# The Hyper API as of writing doesn't offer great hashability for column comparison
# so we create out namedtuple for that purpose
_ColumnType = collections.namedtuple("_ColumnType", ["type_", "nullability"])

TableType = Union[str, tab_api.Name, tab_api.TableName]

_column_types = {
    "int16": _ColumnType(tab_api.SqlType.small_int(), tab_api.Nullability.NOT_NULLABLE),
    "int32": _ColumnType(tab_api.SqlType.int(), tab_api.Nullability.NOT_NULLABLE),
    "int64": _ColumnType(tab_api.SqlType.big_int(), tab_api.Nullability.NOT_NULLABLE),
    "Int16": _ColumnType(tab_api.SqlType.small_int(), tab_api.Nullability.NULLABLE),
    "Int32": _ColumnType(tab_api.SqlType.int(), tab_api.Nullability.NULLABLE),
    "Int64": _ColumnType(tab_api.SqlType.big_int(), tab_api.Nullability.NULLABLE),
    "float32": _ColumnType(tab_api.SqlType.double(), tab_api.Nullability.NULLABLE),
    "float64": _ColumnType(tab_api.SqlType.double(), tab_api.Nullability.NULLABLE),
    "bool": _ColumnType(tab_api.SqlType.bool(), tab_api.Nullability.NOT_NULLABLE),
    "datetime64[ns]": _ColumnType(
        tab_api.SqlType.timestamp(), tab_api.Nullability.NULLABLE
    ),
    "datetime64[ns, UTC]": _ColumnType(
        tab_api.SqlType.timestamp_tz(), tab_api.Nullability.NULLABLE
    ),
    "timedelta64[ns]": _ColumnType(
        tab_api.SqlType.interval(), tab_api.Nullability.NULLABLE
    ),
    "object": _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NULLABLE),
}

if compat.PANDAS_100:
    _column_types["string"] = _ColumnType(
        tab_api.SqlType.text(), tab_api.Nullability.NULLABLE
    )
    _column_types["boolean"] = _ColumnType(
        tab_api.SqlType.bool(), tab_api.Nullability.NULLABLE
    )
else:
    _column_types["object"] = _ColumnType(
        tab_api.SqlType.text(), tab_api.Nullability.NULLABLE
    )

if compat.PANDAS_120:
    _column_types["Float32"] = _ColumnType(
        tab_api.SqlType.double(), tab_api.Nullability.NULLABLE
    )
    _column_types["Float64"] = _ColumnType(
        tab_api.SqlType.double(), tab_api.Nullability.NULLABLE
    )


# Invert this, but exclude float32 as that does not roundtrip
_pandas_types = {v: k for k, v in _column_types.items() if k != "float32"}

# Add things that we can't write to Hyper but can read
_pandas_types[
    _ColumnType(tab_api.SqlType.date(), tab_api.Nullability.NULLABLE)
] = "date"
_pandas_types[
    _ColumnType(tab_api.SqlType.double(), tab_api.Nullability.NOT_NULLABLE)
] = "float64"
if compat.PANDAS_100:
    _pandas_types[
        _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NOT_NULLABLE)
    ] = "string"
else:
    _pandas_types[
        _ColumnType(tab_api.SqlType.text(), tab_api.Nullability.NOT_NULLABLE)
    ] = "object"
