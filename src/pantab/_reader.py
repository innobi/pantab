import pathlib
import shutil
import tempfile
from typing import Literal, Union

import pyarrow as pa
import tableauhyperapi as tab_api

import pantab._types as pantab_types
import pantab.libpantab as libpantab


def frame_from_hyper_query(
    source: Union[str, pathlib.Path],
    query: str,
    *,
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
):
    """See api.rst for documentation."""
    # Call native library to read tuples from result set
    capsule = libpantab.read_from_hyper_query(str(source), query)
    stream = pa.RecordBatchReader._import_from_c_capsule(capsule)
    tbl = stream.read_all()

    if return_type == "pyarrow":
        return tbl
    elif return_type == "polars":
        import polars as pl

        return pl.from_arrow(tbl)
    elif return_type == "pandas":
        import pandas as pd

        return tbl.to_pandas(types_mapper=pd.ArrowDtype)

    raise NotImplementedError("Please choose an appropriate 'return_type' value")


def frame_from_hyper(
    source: Union[str, pathlib.Path],
    *,
    table: pantab_types.TableNameType,
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
):
    """See api.rst for documentation"""
    if isinstance(table, (str, tab_api.Name)) or not table.schema_name:
        table = tab_api.TableName("public", table)

    query = f"SELECT * FROM {table}"
    return frame_from_hyper_query(source, query, return_type=return_type)


def frames_from_hyper(
    source: Union[str, pathlib.Path],
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
):
    """See api.rst for documentation."""
    result = {}

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
            return_type=return_type,
        )

    return result
