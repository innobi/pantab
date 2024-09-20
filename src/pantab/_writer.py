import pathlib
import shutil
import tempfile
import uuid
from typing import Any, Literal, Optional, Union

import pantab._types as pt_types
import pantab.libpantab as libpantab


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
    table: pt_types.TableNameType,
    table_mode: Literal["a", "w"] = "w",
    not_null_columns: Optional[set[str]] = None,
    json_columns: Optional[set[str]] = None,
    geo_columns: Optional[set[str]] = None,
    process_params: Optional[dict[str, str]] = None,
) -> None:
    """See api.rst for documentation"""
    frames_to_hyper(
        {table: df},
        database,
        table_mode=table_mode,
        not_null_columns=not_null_columns,
        json_columns=json_columns,
        geo_columns=geo_columns,
        process_params=process_params,
    )


def frames_to_hyper(
    dict_of_frames: dict[pt_types.TableNameType, Any],
    database: Union[str, pathlib.Path],
    *,
    table_mode: Literal["a", "w"] = "w",
    not_null_columns: Optional[set[str]] = None,
    json_columns: Optional[set[str]] = None,
    geo_columns: Optional[set[str]] = None,
    process_params: Optional[dict[str, str]] = None,
) -> None:
    """See api.rst for documentation."""
    _validate_table_mode(table_mode)

    if not_null_columns is None:
        not_null_columns = set()
    if json_columns is None:
        json_columns = set()
    if geo_columns is None:
        geo_columns = set()
    if process_params is None:
        process_params = {}

    tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

    if table_mode == "a" and pathlib.Path(database).exists():
        shutil.copy(database, tmp_db)

    def convert_to_table_name(table: pt_types.TableNameType):
        if isinstance(table, pt_types.TableauTableName):
            if table.schema_name:
                return (table.schema_name.name.unescaped, table.name.unescaped)
            else:
                return table.name.unescaped
        elif isinstance(table, pt_types.TableauName):
            return table.unescaped

        return table

    data = {
        convert_to_table_name(key): _get_capsule_from_obj(val)
        for key, val in dict_of_frames.items()
    }

    libpantab.write_to_hyper(
        data,
        path=str(tmp_db),
        table_mode=table_mode,
        not_null_columns=not_null_columns,
        json_columns=json_columns,
        geo_columns=geo_columns,
        process_params=process_params,
    )

    # In Python 3.9+ we can just pass the path object, but due to bpo 32689
    # and subsequent typeshed changes it is easier to just pass as str for now
    shutil.move(str(tmp_db), database)
