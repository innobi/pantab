import pathlib
import shutil
import tempfile
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import tableauhyperapi as tab_api

import libreader  # type: ignore
import pantab._types as pantab_types
from pantab._hyper_util import ensure_hyper_process

TableType = Union[str, tab_api.Name, tab_api.TableName]


def _read_query_result(
    result: tab_api.Result,
    dtypes: Optional[Dict[str, str]],
) -> pd.DataFrame:
    if dtypes is None:
        dtypes = {}
        # Construct data types from result
        for column in result.schema.columns:
            # `result.schema` does not provide nullability information.
            # Lwt's err on the safe side and always assume they are nullable
            nullability = tab_api.Nullability.NULLABLE
            column_type = pantab_types._ColumnType(column.type, nullability)
            try:
                dtypes[column.name.unescaped] = pantab_types._pandas_types[column_type]
            except KeyError as e:
                raise TypeError(
                    f"Column {column.name} has unsupported datatype {column.type} "
                    f"with nullability {column.nullability}"
                ) from e

    # Call native library to read tuples from result set
    dtype_strs = tuple(dtypes.values())
    df = pd.DataFrame(libreader.read_hyper_query(result._Result__cdata, dtype_strs))

    df.columns = dtypes.keys()

    # TODO: remove this hackery...
    for k, v in dtypes.items():
        if v == "date":
            dtypes[k] = "datetime64[ns]"

    df = df.astype(dtypes)
    df = df.fillna(value=np.nan)  # Replace any appearances of None

    return df


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
    with connection.execute_query(query) as result:
        return _read_query_result(result, dtypes)


def frame_from_hyper(
    database: Union[str, pathlib.Path],
    *,
    table: TableType,
    hyper_process: Optional[tab_api.HyperProcess] = None,
) -> pd.DataFrame:
    """See api.rst for documentation"""

    with tempfile.TemporaryDirectory() as tmp_dir, ensure_hyper_process(
        hyper_process
    ) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            return _read_table(connection=connection, table=table)


def frames_from_hyper(
    database: Union[str, pathlib.Path],
    *,
    hyper_process: Optional[tab_api.HyperProcess] = None,
) -> Dict[tab_api.TableName, pd.DataFrame]:
    """See api.rst for documentation."""
    result: Dict[TableType, pd.DataFrame] = {}
    with tempfile.TemporaryDirectory() as tmp_dir, ensure_hyper_process(
        hyper_process
    ) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            for schema in connection.catalog.get_schema_names():
                for table in connection.catalog.get_table_names(schema=schema):
                    result[table] = _read_table(connection=connection, table=table)

    return result


def frame_from_hyper_query(
    database: Union[str, pathlib.Path],
    query: str,
    *,
    hyper: Optional[tab_api.HyperProcess] = None,
) -> pd.DataFrame:
    """See api.rst for documentation."""
    with tempfile.TemporaryDirectory() as tmp_dir, ensure_hyper_process(hyper) as hpe:
        tmp_db = shutil.copy(database, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            with connection.execute_query(query) as result:
                return _read_query_result(result, None)
