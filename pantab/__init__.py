from typing import cast, List, Tuple, Union

import pandas as pd
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    TableDefinition,
    TableName,
    Telemetry,
    TypeTag,
)


# pandas type in, tableau type, tab->pan type
_type_mappings = (
    ("int16", TypeTag.SMALL_INT, "int16"),
    ("int32", TypeTag.INT, "int32"),
    ("int64", TypeTag.BIG_INT, "int64"),
    ("float32", TypeTag.DOUBLE, "float64"),
    ("float64", TypeTag.DOUBLE, "float64"),
    ("bool", TypeTag.BOOL, "bool"),
    ("datetime64[ns]", TypeTag.TIMESTAMP, "datetime64[ns]"),
    ("object", TypeTag.TEXT, "object"),
)


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


# Vendored from tableauhypersdk source
insert_functions = {
    TypeTag.UNSUPPORTED: "__write_raw_bytes",
    TypeTag.BOOL: "__write_bool",
    TypeTag.BIG_INT: "__write_big_int",
    TypeTag.SMALL_INT: "__write_small_int",
    TypeTag.INT: "__write_int",
    TypeTag.DOUBLE: "__write_double",
    TypeTag.OID: "__write_uint",
    TypeTag.BYTES: "__write_bytes",
    TypeTag.TEXT: "__write_text",
    TypeTag.VARCHAR: "__write_text",
    TypeTag.CHAR: "__write_text",
    TypeTag.JSON: "__write_text",
    TypeTag.DATE: "__write_date",
    TypeTag.INTERVAL: "__write_interval",
    TypeTag.TIME: "__write_time",
    TypeTag.TIMESTAMP: "__write_timestamp",
    TypeTag.TIMESTAMP_TZ: "__write_timestamp",
    TypeTag.GEOGRAPHY: "__write_bytes",
}


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


def frame_to_hyper(df: pd.DataFrame, fn: str, table_name: str) -> None:
    """
    Convert a DataFrame to a .hyper extract.
    """
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, "myapp") as hpe:
        with Connection(hpe.endpoint, fn, CreateMode.CREATE_AND_REPLACE) as conn:
            table_def = TableDefinition(name=TableName(table_name, table_name))

            ttypes = _types_for_columns(df)
            for col_name, ttype in zip(list(df.columns), ttypes):
                col = TableDefinition.Column(col_name, ttype)
            table_def.add_column(col)

            conn.catalog.create_table(table_def)

            with Inserter(conn, table_def) as inserter:
                insert_funcs = tuple(insert_functions[ttype] for ttype in ttypes)
                for row in df.itertuples(index=False):
                    for index, val in enumerate(row):
                        getattr(inserter, insert_funcs[index])(val)

                inserter.execute()


def frame_from_hyper(fn: str, table_name: str = "Extract") -> pd.DataFrame:
    """
    Extracts a DataFrame from a .hyper extract.
    """
    raise NotImplementedError("Not possible with current SDK")
