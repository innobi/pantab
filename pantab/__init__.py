from typing import cast, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
    TypeTag,
)


__all__ = ["frame_to_hyper", "frame_from_hyper"]


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


def _pandas_to_tableau_type(typ: str) -> TypeTag:
    for ptype, ttype, _ in _type_mappings:
        if typ == ptype:
            return ttype

    raise TypeError("Conversion of '{}' dtypes not yet supported!".format(typ))


def _tableau_to_pandas_type(typ: TypeTag) -> str:
    for _, ttype, ret_type in _type_mappings:
        if typ == ttype:
            return ret_type

    # Fallback to object
    return "object"


def _types_for_columns(df: pd.DataFrame) -> Tuple[int, ...]:
    """
    Return a tuple of Tableau types matching the ordering of `df.columns`.
    """
    return tuple(_pandas_to_tableau_type(df[x].dtype.name) for x in df.columns)


# The Hyper API doesn't expose these functions directly and wraps them with
# validation; we can skip the validation because the column dtypes enforce that
_insert_functions = {
    TypeTag.UNSUPPORTED: "_Inserter__write_raw_bytes",
    TypeTag.BOOL: "_Inserter__write_bool",
    TypeTag.BIG_INT: "_Inserter__write_big_int",
    TypeTag.SMALL_INT: "_Inserter__write_small_int",
    TypeTag.INT: "_Inserter__write_int",
    TypeTag.DOUBLE: "_Inserter__write_double",
    TypeTag.OID: "_Inserter__write_uint",
    TypeTag.BYTES: "_Inserter__write_bytes",
    TypeTag.TEXT: "_Inserter__write_text",
    TypeTag.VARCHAR: "_Inserter__write_text",
    TypeTag.CHAR: "_Inserter__write_text",
    TypeTag.JSON: "_Inserter__write_text",
    TypeTag.DATE: "_Inserter__write_date",
    TypeTag.INTERVAL: "_Inserter__write_interval",
    TypeTag.TIME: "_Inserter__write_time",
    TypeTag.TIMESTAMP: "_Inserter__write_timestamp",
    TypeTag.TIMESTAMP_TZ: "_Inserter__write_timestamp",
    TypeTag.GEOGRAPHY: "_Inserter__write_bytes",
}


def frame_to_hyper(
    df: pd.DataFrame, database: str, *, table: str, schema: Optional[str] = None
) -> None:
    """
    Convert a DataFrame to a .hyper extract.

    Parameters
    ----------
    df : DataFrame
        Data to be written out.
    database : str
        Name / location of the Hyper file to be written to.
    table : str
        Name of the table to write to. Must be supplied as a keyword argument.
    schema : str, optional
        Schema name to write to. Must be supplied as a keyword argument.
    """
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hpe:
        with Connection(hpe.endpoint, database, CreateMode.CREATE_AND_REPLACE) as conn:

            table_def = TableDefinition(name=TableName(schema, table))

            ttypes = _types_for_columns(df)
            for col_name, ttype in zip(list(df.columns), ttypes):
                col = TableDefinition.Column(col_name, SqlType(ttype))
                table_def.add_column(col)

            if schema:
                conn.catalog.create_schema_if_not_exists(schema)
            conn.catalog.create_table_if_not_exists(table_def)

            with Inserter(conn, table_def) as inserter:
                insert_funcs = tuple(_insert_functions[ttype] for ttype in ttypes)
                for row in df.itertuples(index=False):
                    for index, val in enumerate(row):
                        # Missing value handling
                        if val is None or val != val:
                            inserter._Inserter__write_null()
                        else:
                            getattr(inserter, insert_funcs[index])(val)

                inserter.execute()


def frame_from_hyper(
    database: str, table: str, schema: Optional[str] = None
) -> pd.DataFrame:
    """
    Extracts a DataFrame from a .hyper extract.

    Parameters
    ----------
    database : str
        Name / location of the Hyper file to be read.
    table : str
        Name of the table to read. Must be supplied as a keyword argument.
    schema : str, optional
        Schema to read table from. Must be supplied as a keyword argument.

    Returns
    -------
    DataFrame
    """
    target = table
    if schema:
        target = f"{schema}.{target}"

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hpe:
        with Connection(hpe.endpoint, database) as conn:
            with conn.execute_query(f"SELECT * from {target}") as result:
                schema = result.schema
                # Create list containing column name as key, pandas dtype as value
                dtypes = {}  # Dict[str, str]
                for column in schema.columns:
                    dtypes[column.name.unescaped] = _tableau_to_pandas_type(
                        column.type.tag
                    )

                df = pd.DataFrame(result)

    df.columns = dtypes.keys()
    # The tableauhyperapi.Timestamp class is not implicitly convertible to a datetime
    # so we need to run an apply against applicable types
    for key, val in dtypes.items():
        if val == "datetime64[ns]":
            df[key] = df[key].apply(lambda x: x._to_datetime())

    df = df.astype(dtypes)
    df = df.fillna(value=np.nan)  # Replace any appearances of None

    return df
