import pathlib
from typing import Literal, Optional, Union

import pyarrow as pa

import pantab._types as pt_types
import pantab.libpantab as libpantab


class PantabStream:
    """
    This class adheres to the Arrow PyCapsule interface.
    """

    def __init__(self, capsule):
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema=None):
        if requested_schema is not None:
            raise NotImplementedError("pantab does not support requested_schema!")

        return self._capsule


def frame_from_hyper_query(
    source: Union[str, pathlib.Path],
    query: str,
    *,
    return_type: Literal["pandas", "polars", "pyarrow", "stream"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
    chunk_size=0,
):
    """
    Executes a SQL query and returns the result as a pandas dataframe

    :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
    :param query: SQL query to execute.
    :param return_type: The type of result to be returned
    :param process_params: Parameters to pass to the Hyper Process constructor.
    :param chunk_size: When returning a stream, the number of rows in each chunk to be read
    """
    if chunk_size and return_type != "stream":
        raise NotImplementedError(
            "Chunking support is only implemented with return_type='stream'"
        )

    if process_params is None:
        process_params = {}

    # Call native library to read tuples from result set
    capsule = libpantab.read_from_hyper_query(
        str(source), query, process_params, chunk_size
    )

    if return_type == "stream":
        return PantabStream(capsule)

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
    return_type: Literal["pandas", "polars", "pyarrow", "stream"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
    chunk_size=0,
):
    """
    Extracts a DataFrame from a .hyper extract.

    :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
    :param table: Table to read.
    :param return_type: The type of DataFrame to be returned
    :param process_params: Parameters to pass to the Hyper Process constructor.
    :param chunk_size: When returning a stream, the number of rows in each chunk to be read
    """
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
        source,
        query,
        return_type=return_type,
        process_params=process_params,
        chunk_size=chunk_size,
    )


def frames_from_hyper(
    source: Union[str, pathlib.Path],
    return_type: Literal["pandas", "polars", "pyarrow", "stream"] = "pandas",
    process_params: Optional[dict[str, str]] = None,
    chunk_size=0,
):
    """
    Extracts tables from a .hyper extract.

    :param source: Name / location of the Hyper file to be read  or Hyper-API connection.
    :param return_type: The type of DataFrame to be returned
    :param process_params: Parameters to pass to the Hyper Process constructor.
    :param chunk_size: When returning a stream, the number of rows in each chunk to be read
    """
    result = {}

    table_names = libpantab.get_table_names(str(source))
    for table in table_names:
        result[table] = frame_from_hyper(
            source=source,
            table=table,
            return_type=return_type,
            process_params=process_params,
            chunk_size=chunk_size,
        )

    return result
