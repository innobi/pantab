import pathlib
import shutil
import tempfile
from typing import Dict, Union

import numpy as np
import pandas as pd
import tableauhyperapi as tab_api

import libreader  # type: ignore
import pantab._types as pantab_types

TableType = Union[str, tab_api.Name, tab_api.TableName]


def _read_table(*, connection: tab_api.Connection, table: TableType) -> pd.DataFrame:
    if isinstance(table, str):
        table = tab_api.TableName(table)

    table_def = connection.catalog.get_table_definition(table)
    columns = table_def.columns

    dtypes: Dict[str, str] = {}
    for column in columns:
        column_type = pantab_types._ColumnType(column.type, column.nullability)
        try:
            dtypes[column.name.unescaped] = pantab_types._pandas_types[column_type]
        except KeyError as e:
            raise TypeError(
                f"Column {column.name} has unsupported datatype {column.type} "
                f"with nullability {column.nullability}"
            ) from e

    query = f"SELECT * from {table}"
    dtype_strs = tuple(dtypes.values())

    df = pd.DataFrame(libreader.read_hyper_query(connection._cdata, query, dtype_strs))

    df.columns = dtypes.keys()

    # TODO: remove this hackery...
    for k, v in dtypes.items():
        if v == "date":
            dtypes[k] = "datetime64[ns]"

    df = df.astype(dtypes)
    df = df.fillna(value=np.nan)  # Replace any appearances of None

    return df


def frame_from_hyper(
    database: Union[str, pathlib.Path], *, table: TableType
) -> pd.DataFrame:
    """See api.rst for documentation"""
    with tempfile.TemporaryDirectory() as tmp_dir, tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            return _read_table(connection=connection, table=table)


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
