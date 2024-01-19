import pathlib
import shutil
import tempfile
import uuid
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import tableauhyperapi as tab_api

import pantab._types as pantab_types
import pantab.src.pantab as libpantab  # type: ignore


def _validate_table_mode(table_mode: str) -> None:
    if table_mode not in {"a", "w"}:
        raise ValueError("'table_mode' must be either 'w' or 'a'")


def frame_to_hyper(
    df: pd.DataFrame,
    database: Union[str, pathlib.Path],
    *,
    table: pantab_types.TableType,
    table_mode: str = "w",
) -> None:
    """See api.rst for documentation"""
    frames_to_hyper(
        {table: df},
        database,
        table_mode,
    )


def frames_to_hyper(
    dict_of_frames: dict[pantab_types.TableType, pd.DataFrame],
    database: Union[str, pathlib.Path],
    table_mode: str = "w",
    *,
    hyper_process: Optional[tab_api.HyperProcess] = None,
) -> None:
    """See api.rst for documentation."""
    _validate_table_mode(table_mode)

    tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

    if table_mode == "a" and pathlib.Path(database).exists():
        shutil.copy(database, tmp_db)

    def convert_to_table_name(table: pantab_types.TableType):
        # nanobind expects a tuple of (schema, table) strings
        if isinstance(table, (str, tab_api.Name)) or not table.schema_name:
            table = tab_api.TableName("public", table)

        return (table.schema_name.name.unescaped, table.name.unescaped)

    data = {
        convert_to_table_name(key): pa.Table.from_pandas(val)
        for key, val in dict_of_frames.items()
    }
    libpantab.write_to_hyper(data, path=str(tmp_db), table_mode=table_mode)

    # In Python 3.9+ we can just pass the path object, but due to bpo 32689
    # and subsequent typeshed changes it is easier to just pass as str for now
    shutil.move(str(tmp_db), database)
