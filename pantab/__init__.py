from collections import OrderedDict
from typing import cast, List, Tuple, Union

import pandas as pd
from tableausdk.Types import Type as ttypes
import tableausdk.HyperExtract as hpe


# pandas type in, tableau type, tab->pan type
_type_mappings = (
    ("int16", ttypes.INTEGER, "int64"),
    ("int32", ttypes.INTEGER, "int64"),
    ("int64", ttypes.INTEGER, "int64"),
    ("float32", ttypes.DOUBLE, "float64"),
    ("float64", ttypes.DOUBLE, "float64"),
    ("bool", ttypes.BOOLEAN, "bool"),
    ("datetime64[ns]", ttypes.DATETIME, "datetime64[ns]"),
    # ('timedelta64[ns]', ttypes.DURATION, 'timedelta64[ns]'),
    ("object", ttypes.UNICODE_STRING, "object"),
)


_type_accessors = {
    ttypes.BOOLEAN: "setBoolean",
    ttypes.DATETIME: "setDateTime",
    ttypes.DOUBLE: "setDouble",
    ttypes.DURATION: "setDuration",
    ttypes.INTEGER: "setInteger",
    ttypes.UNICODE_STRING: "setString",
}


def pandas_to_tableau_type(typ: str) -> int:
    for ptype, ttype, _ in _type_mappings:
        if typ == ptype:
            return ttype

    raise TypeError("Conversion of '{}' dtypes not yet supported!".format(typ))


def tableau_to_pandas_type(typ: int) -> str:
    for _, ttype, ret_type in _type_mappings:
        if typ == ttype:
            return ret_type

    # Fallback to object
    return "object"


def _types_for_columns(df: pd.DataFrame) -> Tuple[int, ...]:
    """
    Return a tuple of Tableau types matching the ordering of `df.columns`.
    """
    return tuple(pandas_to_tableau_type(df[x].dtype.name) for x in df.columns)


def _accessor_for_tableau_type(typ: int) -> str:
    return _type_accessors[typ]


def _append_args_for_val_and_accessor(
    arg_l: List, val: Union[str, pd.Timestamp], accessor: str
) -> None:
    """
    Dynamically append to args depending on the needs of `accessor`
    """
    # Conditional branch can certainly be refactored, but going the
    # easy route for the time being
    if accessor == "setDateTime":
        val = cast(pd.Timestamp, val)
        for window in ("year", "month", "day", "hour", "minute", "second"):
            arg_l.append(getattr(val, window))
        # last positional arg to func must be in tenth of ms
        # will lose precision compared to pandas type
        arg_l.append(val.microsecond // 100)
        """  Durations weren't working; may be an SDK bug?
        elif accessor == 'setDuration':
            for window in ('days', 'hours', 'minutes', 'seconds'):
                arg_l.append(getattr(val.components, window))
            # last positional arg to func must be in tenth of ms
            # will lose precision compared to pandas type
            arg_l.append(val.microseconds // 100)
        """
    else:
        arg_l.append(val)


def frame_to_hyper(df: pd.DataFrame, fn: str, table_name: str = "Extract") -> None:
    """
    Convert a DataFrame to a .hyper extract.
    """
    if table_name != "Extract":
        raise ValueError(
            "The Tableau SDK currently only supports a table name " "of 'Extract'"
        )

    schema = hpe.TableDefinition()
    ttypes = _types_for_columns(df)
    for col, ttype in zip(list(df.columns), ttypes):
        schema.addColumn(col, ttype)

    rows = []
    accessors = tuple(_accessor_for_tableau_type(ttype) for ttype in ttypes)
    for tup in df.itertuples(index=False):
        row = hpe.Row(schema)
        for index, accessor in enumerate(accessors):
            val = tup[index]
            fn_args = [index]
            _append_args_for_val_and_accessor(fn_args, val, accessor)
            getattr(row, accessor)(*fn_args)

        rows.append(row)

    with hpe.Extract(fn) as extract:
        table = extract.addTable(table_name, schema)
        for row in rows:
            table.insert(row)


def frame_from_hyper(fn: str, table_name: str = "Extract") -> pd.DataFrame:
    """
    Extracts a DataFrame from a .hyper extract.
    """
    if table_name != "Extract":
        raise ValueError(
            "The Tableau SDK currently only supports a table name " "of 'Extract'"
        )

    raise NotImplementedError("Not possible with current SDK")

    with hpe.Extract(fn) as extract:
        tbl = extract.openTable(table_name)

    schema = tbl.getTableDefinition()
    col_types = OrderedDict()

    # __iter__ support would be ideal here...
    col_cnt = schema.getColumnCount()
    for i in range(col_cnt):
        col_types[schema.getColumnName(i)] = tableau_to_pandas_type(
            schema.getColumnType(i)
        )

    # Let's now build out our frame
    df = pd.DataFrame(columns=col_types.keys()).astype(col_types)

    # It's not yet available in the SDK, but below is where we would
    # iterate the rows and populate our DataFrame
