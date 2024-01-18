import pathlib
import shutil
import tempfile
from typing import Dict, Optional, Union

import pandas as pd
import pyarrow as pa
import tableauhyperapi as tab_api

import pantab.src.pantab as libpantab  # type: ignore

TableType = Union[str, tab_api.Name, tab_api.TableName]


def frame_from_hyper(
    source: Union[str, pathlib.Path],
    *,
    table: TableType,
) -> pd.DataFrame:
    """See api.rst for documentation"""
    if isinstance(table, (str, tab_api.Name)) or not table.schema_name:
        table = tab_api.TableName("public", table)

    query = f"SELECT * FROM {table}"
    return frame_from_hyper_query(source, query)


def frames_from_hyper(
    source: Union[str, pathlib.Path],
) -> Dict[tab_api.TableName, pd.DataFrame]:
    """See api.rst for documentation."""
    result: Dict[TableType, pd.DataFrame] = {}

    table_names = []
    with tempfile.TemporaryDirectory() as tmp_dir, tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = shutil.copy(source, tmp_dir)
        with tab_api.Connection(hpe.endpoint, tmp_db) as connection:
            for schema in connection.catalog.get_schema_names():
                for table in connection.catalog.get_table_names(schema=schema):
                    table_names.append(table)

    for table in table_names:
        result[table] = frame_from_hyper(
            source=source,
            table=table,
        )

    return result


def frame_from_hyper_query(
    source: Union[str, pathlib.Path],
    query: str,
    *,
    hyper_process: Optional[tab_api.HyperProcess] = None,
) -> pd.DataFrame:
    """See api.rst for documentation."""
    # Call native library to read tuples from result set
    capsule = libpantab.read_from_hyper_query(str(source), query)
    stream = pa.RecordBatchReader._import_from_c_capsule(capsule)
    df = stream.read_pandas()

    return df
