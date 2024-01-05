import pathlib
import shutil
import tempfile
from typing import Dict, Optional, Union

import pandas as pd
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

    data, columns, dtypes = libpantab.read_from_hyper_table(
        str(source),
        table.schema_name.name.unescaped,  # TODO: this probably allows injection
        table.name.unescaped,
    )
    df = pd.DataFrame(data, columns=columns)
    dtype_map = {k: v for k, v in zip(columns, dtypes) if v != "datetime64[ns, UTC]"}
    df = df.astype(dtype_map)

    tz_aware_columns = {
        col for col, dtype in zip(columns, dtypes) if dtype == "datetime64[ns, UTC]"
    }
    for col in tz_aware_columns:
        try:
            df[col] = df[col].dt.tz_localize("UTC")
        except AttributeError:  # happens when df[col] is empty
            df[col] = df[col].astype("datetime64[ns, UTC]")

    return df


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
    df = pd.DataFrame(libpantab.read_from_hyper_query(str(source), query))
    data, columns, dtypes = libpantab.read_from_hyper_query(str(source), query)
    df = pd.DataFrame(data, columns=columns)
    dtype_map = {k: v for k, v in zip(columns, dtypes) if v != "datetime64[ns, UTC]"}
    df = df.astype(dtype_map)

    tz_aware_columns = {
        col for col, dtype in zip(columns, dtypes) if dtype == "datetime64[ns, UTC]"
    }
    for col in tz_aware_columns:
        try:
            df[col] = df[col].dt.tz_localize("UTC")
        except AttributeError:  # happens when df[col] is empty
            df[col] = df[col].astype("datetime64[ns, UTC]")

    return df
