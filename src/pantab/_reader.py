import pathlib
from typing import Literal, Optional, Union

import pyarrow as pa

import pantab._types as pt_types
import pantab.libpantab as libpantab


def frame_from_hyper_query(
    source: Union[str, pathlib.Path],
    query: str,
    *,
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
):
    """See api.rst for documentation."""
    if process_params is None:
        process_params = {}

    # Call native library to read tuples from result set
    capsule = libpantab.read_from_hyper_query(str(source), query, process_params)
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
    table: pt_types.TableNameType,
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
):
    """See api.rst for documentation"""
    if isinstance(table, (pt_types.TableauName, pt_types.TableauTableName)):
        tbl = str(table)
    elif isinstance(table, tuple):
        tbl = ".".join(
            libpantab.escape_sql_identifier(x) for x in table
        )  # check for injection
    else:
        tbl = libpantab.escape_sql_identifier(table)

    query = f"SELECT * FROM {tbl}"
    return frame_from_hyper_query(
        source, query, return_type=return_type, process_params=process_params
    )


def frames_from_hyper(
    source: Union[str, pathlib.Path],
    return_type: Literal["pandas", "polars", "pyarrow"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
):
    """See api.rst for documentation."""
    result = {}

    table_names = libpantab.get_table_names(str(source))
    for table in table_names:
        result[table] = frame_from_hyper(
            source=source,
            table=table,
            return_type=return_type,
            process_params=process_params,
        )

    return result
