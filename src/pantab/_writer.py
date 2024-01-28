import pathlib
import shutil
import tempfile
import uuid
from typing import Any, Literal, Union

import tableauhyperapi as tab_api

import pantab._types as pantab_types
import pantab.libpantab as libpantab  # type: ignore


def _validate_table_mode(table_mode: Literal["a", "w"]) -> None:
    if table_mode not in {"a", "w"}:
        raise ValueError("'table_mode' must be either 'w' or 'a'")


def _get_capsule_from_obj(obj):
    """Returns the Arrow capsule underlying obj"""
    # Check first for the Arrow C Data Interface compliance
    if hasattr(obj, "__arrow_c_stream__"):
        return obj.__arrow_c_stream__()

    # pandas < 3.0 did not have the Arrow C Data Interface, so
    # convert to PyArrow
    try:
        import pandas as pd
        import pyarrow as pa

        if isinstance(obj, pd.DataFrame):
            return pa.Table.from_pandas(obj).__arrow_c_stream__()
    except ModuleNotFoundError:
        pass

    # see polars GH issue #12530 - PyCapsule interface not yet developed
    try:
        import polars as pl

        if isinstance(obj, pl.DataFrame):
            return obj.to_arrow().__arrow_c_stream__()
    except ModuleNotFoundError:
        pass

    # More introspection could happen in the future...but end with TypeError if we
    # can not find what we are looking for
    raise TypeError(
        f"Could not convert object of type '{type(obj)}' to Arrow C Data Interface"
    )


def frame_to_hyper(
    df,
    database: Union[str, pathlib.Path],
    *,
    table: pantab_types.TableType,
    table_mode: Literal["a", "w"] = "w",
    json_columns: list[str] = None,
    geo_columns: list[str] = None,
) -> None:
    """See api.rst for documentation"""
    frames_to_hyper(
        {table: df},
        database,
        table_mode=table_mode,
        json_columns=json_columns,
        geo_columns=geo_columns,
    )


def frames_to_hyper(
    dict_of_frames: dict[pantab_types.TableType, Any],
    database: Union[str, pathlib.Path],
    *,
    table_mode: Literal["a", "w"] = "w",
    json_columns: set[str] = None,
    geo_columns: set[str] = None,
) -> None:
    """See api.rst for documentation."""
    _validate_table_mode(table_mode)
    if json_columns is None:
        json_columns = set()
    if geo_columns is None:
        geo_columns = set()

    tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

    if table_mode == "a" and pathlib.Path(database).exists():
        shutil.copy(database, tmp_db)

    def convert_to_table_name(table: pantab_types.TableType):
        # nanobind expects a tuple of (schema, table) strings
        if isinstance(table, (str, tab_api.Name)) or not table.schema_name:
            table = tab_api.TableName("public", table)

        return (table.schema_name.name.unescaped, table.name.unescaped)

    data = {
        convert_to_table_name(key): _get_capsule_from_obj(val)
        for key, val in dict_of_frames.items()
    }

    libpantab.write_to_hyper(
        data,
        path=str(tmp_db),
        table_mode=table_mode,
        json_columns=json_columns,
        geo_columns=geo_columns,
    )

    # In Python 3.9+ we can just pass the path object, but due to bpo 32689
    # and subsequent typeshed changes it is easier to just pass as str for now
    shutil.move(str(tmp_db), database)
