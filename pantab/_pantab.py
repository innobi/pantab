import pathlib
import shutil
import tempfile
import uuid
from typing import Dict, Tuple, Union

import numpy as np
import pandas as pd
import tableauhyperapi as tab_api

__all__ = ["frame_to_hyper", "frame_from_hyper", "frames_from_hyper", "frames_to_hyper"]


# pandas type in, tableau type, tab->pan type
_type_mappings = (
    ("int16", tab_api.TypeTag.SMALL_INT, "int16"),
    ("int32", tab_api.TypeTag.INT, "int32"),
    ("int64", tab_api.TypeTag.BIG_INT, "int64"),
    ("float32", tab_api.TypeTag.DOUBLE, "float64"),
    ("float64", tab_api.TypeTag.DOUBLE, "float64"),
    ("bool", tab_api.TypeTag.BOOL, "bool"),
    ("datetime64[ns]", tab_api.TypeTag.TIMESTAMP, "datetime64[ns]"),
    ("timedelta64[ns]", tab_api.TypeTag.INTERVAL, "timedelta64[ns]"),
    ("object", tab_api.TypeTag.TEXT, "object"),
)


TableType = Union[str, tab_api.Name, tab_api.TableName]


def _pandas_to_tableau_type(typ: str) -> tab_api.TypeTag:
    for ptype, ttype, _ in _type_mappings:
        if typ == ptype:
            return ttype

    raise TypeError("Conversion of '{}' dtypes not supported!".format(typ))


def _tableau_to_pandas_type(typ: tab_api.TypeTag) -> str:
    for _, ttype, ret_type in _type_mappings:
        if typ == ttype:
            return ret_type

    # Fallback to object
    return "object"


def _types_for_columns(df: pd.DataFrame) -> Tuple[tab_api.TypeTag, ...]:
    """
    Return a tuple of Tableau types matching the ordering of `df.columns`.
    """
    return tuple(_pandas_to_tableau_type(x.name) for x in df.dtypes)


# The Hyper API doesn't expose these functions directly and wraps them with
# validation; we can skip the validation because the column dtypes enforce that
_insert_functions = {
    tab_api.TypeTag.UNSUPPORTED: "_Inserter__write_raw_bytes",
    tab_api.TypeTag.BOOL: "_Inserter__write_bool",
    tab_api.TypeTag.BIG_INT: "_Inserter__write_big_int",
    tab_api.TypeTag.SMALL_INT: "_Inserter__write_small_int",
    tab_api.TypeTag.INT: "_Inserter__write_int",
    tab_api.TypeTag.DOUBLE: "_Inserter__write_double",
    tab_api.TypeTag.OID: "_Inserter__write_uint",
    tab_api.TypeTag.BYTES: "_Inserter__write_bytes",
    tab_api.TypeTag.TEXT: "_Inserter__write_text",
    tab_api.TypeTag.VARCHAR: "_Inserter__write_text",
    tab_api.TypeTag.CHAR: "_Inserter__write_text",
    tab_api.TypeTag.JSON: "_Inserter__write_text",
    tab_api.TypeTag.DATE: "_Inserter__write_date",
    tab_api.TypeTag.INTERVAL: "_Inserter__write_interval",
    tab_api.TypeTag.TIME: "_Inserter__write_time",
    tab_api.TypeTag.TIMESTAMP: "_Inserter__write_timestamp",
    tab_api.TypeTag.TIMESTAMP_TZ: "_Inserter__write_timestamp",
    tab_api.TypeTag.GEOGRAPHY: "_Inserter__write_bytes",
}


def _timedelta_to_interval(td: pd.Timedelta) -> tab_api.Interval:
    """Converts a pandas Timedelta to tableau Hyper API implementation."""
    days = td.days
    without_days = td - pd.Timedelta(days=days)
    total_seconds = int(without_days.total_seconds())
    microseconds = total_seconds * 1_000_000

    return tab_api.Interval(months=0, days=days, microseconds=microseconds)


def _interval_to_timedelta(interval: tab_api.Interval) -> pd.Timedelta:
    """Converts a tableau Hyper API Interval to a pandas Timedelta."""
    if interval.months != 0:
        raise ValueError("Cannot read Intervals with month componenets.")

    return pd.Timedelta(days=interval.days, microseconds=interval.microseconds)


def _insert_frame(
    df: pd.DataFrame, *, connection: tab_api.Connection, table: TableType
) -> None:
    if isinstance(table, str):
        table = tab_api.TableName(table)

    table_def = tab_api.TableDefinition(name=table)
    ttypes = _types_for_columns(df)
    for col_name, ttype in zip(list(df.columns), ttypes):
        col = tab_api.TableDefinition.Column(col_name, tab_api.SqlType(ttype))
        table_def.add_column(col)

    if isinstance(table, tab_api.TableName) and table.schema_name:
        connection.catalog.create_schema_if_not_exists(table.schema_name)

    connection.catalog.create_table_if_not_exists(table_def)

    # Special handling for conversions
    df = df.copy()
    for index, (_, content) in enumerate(df.items()):
        if content.dtype == "timedelta64[ns]":
            df.iloc[:, index] = content.apply(_timedelta_to_interval)

    with tab_api.Inserter(connection, table_def) as inserter:
        insert_funcs = tuple(_insert_functions[ttype] for ttype in ttypes)
        for row in df.itertuples(index=False):
            for index, val in enumerate(row):
                # Missing value handling
                if val is None or val != val:
                    inserter._Inserter__write_null()
                else:
                    getattr(inserter, insert_funcs[index])(val)

        inserter.execute()


def _read_table(*, connection: tab_api.Connection, table: TableType) -> pd.DataFrame:
    if isinstance(table, str):
        table = tab_api.TableName(table)

    with connection.execute_query(f"SELECT * from {table}") as result:
        schema = result.schema
        # Create list containing column name as key, pandas dtype as value
        dtypes: Dict[str, str] = {}
        for column in schema.columns:
            dtypes[column.name.unescaped] = _tableau_to_pandas_type(column.type.tag)

        df = pd.DataFrame(result)

    df.columns = dtypes.keys()
    # The tableauhyperapi.Timestamp class is not implicitly convertible to a datetime
    # so we need to run an apply against applicable types
    for key, val in dtypes.items():
        if val == "datetime64[ns]":
            df[key] = df[key].apply(lambda x: x._to_datetime())
        elif val == "timedelta64[ns]":
            df[key] = df[key].apply(_interval_to_timedelta)

    df = df.astype(dtypes)
    df = df.fillna(value=np.nan)  # Replace any appearances of None

    return df


def frame_to_hyper(
    df: pd.DataFrame, database: Union[str, pathlib.Path], *, table: TableType
) -> None:
    """
    Convert a DataFrame to a .hyper extract.

    Parameters
    ----------
    df : DataFrame
        Data to be written out.
    database : str
        tab_api.Name / location of the Hyper file to be written to.
    table : str, tab_api.Name or tab_api.TableName
        tab_api.Name of the table to write to. Must be supplied as a keyword argument.
    """
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

        with tab_api.Connection(
            hpe.endpoint, tmp_db, tab_api.CreateMode.CREATE
        ) as connection:
            _insert_frame(df, connection=connection, table=table)

        shutil.move(tmp_db, database)


def frame_from_hyper(
    database: Union[str, pathlib.Path], *, table: TableType
) -> pd.DataFrame:
    """
    Extracts a DataFrame from a .hyper extract.

    Parameters
    ----------
    database : str
        tab_api.Name / location of the Hyper file to be read.
    table : str
        tab_api.Name of the table to read. Must be supplied as a keyword argument.

    Returns
    -------
    DataFrame
    """
    with tempfile.TemporaryDirectory() as tmp_dir, tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            return _read_table(connection=connection, table=table)


def frames_to_hyper(
    dict_of_frames: Dict[TableType, pd.DataFrame], database: Union[str, pathlib.Path]
) -> None:
    """See api.rst for documentation."""
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

        with tab_api.Connection(
            hpe.endpoint, tmp_db, tab_api.CreateMode.CREATE
        ) as connection:
            for table, df in dict_of_frames.items():
                _insert_frame(df, connection=connection, table=table)

        shutil.move(tmp_db, database)


def frames_from_hyper(
    database: Union[str, pathlib.Path]
) -> Dict[tab_api.TableName, pd.DataFrame]:
    """See api.rst for documentation."""
    result: Dict[TableType, pd.DataFrame] = {}
    with tempfile.TemporaryDirectory() as tmp_dir, tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            for schema in connection.catalog.get_schema_names():
                for table in connection.catalog.get_table_names(schema=schema):
                    result[table] = _read_table(connection=connection, table=table)

    return result
