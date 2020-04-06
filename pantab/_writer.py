import itertools
import pathlib
import shutil
import tempfile
import uuid
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import tableauhyperapi as tab_api

import libwriter  # type: ignore
import pantab._types as pantab_types


def _pandas_to_tableau_type(typ: str) -> pantab_types._ColumnType:
    try:
        return pantab_types._column_types[typ]
    except KeyError:
        raise TypeError("Conversion of '{}' dtypes not supported!".format(typ))


def _timedelta_to_interval(td: pd.Timedelta) -> Optional[tab_api.Interval]:
    """Converts a pandas Timedelta to tableau Hyper API implementation."""
    if pd.isnull(td):
        return None

    days = td.days
    without_days = td - pd.Timedelta(days=days)
    total_seconds = int(without_days.total_seconds())
    microseconds = total_seconds * 1_000_000

    return tab_api.Interval(months=0, days=days, microseconds=microseconds)


def _validate_table_mode(table_mode: str) -> None:
    if table_mode not in {"a", "w"}:
        raise ValueError("'table_mode' must be either 'w' or 'a'")


def _assert_columns_equal(
    left: Sequence[tab_api.TableDefinition.Column],
    right: Sequence[tab_api.TableDefinition.Column],
) -> None:
    """
    Helper function to validate if sequences of columns are equal.

    The TableauHyperAPI as of 0.0.8953 does not implement equality operations
    for Column instances, hence the need for this.
    """

    class DummyColumn:
        """Dummy class to match items needed for str repr of columns."""

        @property
        def name(self):
            return None

        @property
        def type(self):
            return None

        @property
        def nullability(self):
            return None

    for c1, c2 in itertools.zip_longest(left, right, fillvalue=DummyColumn()):
        if c1.name != c2.name or c1.type != c2.type or c1.nullability != c2.nullability:
            break  # go to error handler
    else:
        return None  # everything matched up, so bail out

    c1_str = ", ".join(
        f"(Name={x.name}, Type={x.type}, Nullability={x.nullability})" for x in left
    )
    c2_str = ", ".join(
        f"(Name={x.name}, Type={x.type}, Nullability={x.nullability})" for x in right
    )

    raise TypeError(f"Mismatched column definitions: {c1_str} != {c2_str}")


def _maybe_convert_timedelta(df: pd.DataFrame) -> Tuple[pd.DataFrame, Tuple[str, ...]]:
    """
    Hyper uses a different storage format than pandas / Python for timedeltas.

    Ultimately this should be pushed to the C extension, but doesn't look to fully work
    at the moment anyway so keep in Python until complete.
    """
    orig_dtypes = tuple(map(str, df.dtypes))
    deltas = df.select_dtypes(include=["timedelta64[ns]"])

    if deltas.empty:
        pass
    else:
        df = df.copy()

        for index, (_, content) in enumerate(df.items()):
            if content.dtype == "timedelta64[ns]":
                df.iloc[:, index] = content.apply(_timedelta_to_interval)

    return df, orig_dtypes


def _insert_frame(
    df: pd.DataFrame,
    *,
    connection: tab_api.Connection,
    table: pantab_types.TableType,
    table_mode: str,
) -> None:
    _validate_table_mode(table_mode)

    if isinstance(table, str):
        table = tab_api.TableName(table)

    # Populate insertion mechanisms dependent on column types
    column_types: List[pantab_types._ColumnType] = []
    columns: List[tab_api.TableDefinition.Column] = []
    for col_name, dtype in df.dtypes.items():
        column_type = _pandas_to_tableau_type(dtype.name)
        column_types.append(column_type)
        columns.append(
            tab_api.TableDefinition.Column(
                name=col_name,
                type=column_type.type_,
                nullability=column_type.nullability,
            )
        )

    # Sanity check for existing table structures
    if table_mode == "a" and connection.catalog.has_table(table):
        table_def = connection.catalog.get_table_definition(table)
        _assert_columns_equal(columns, table_def.columns)
    else:  # New table, potentially new schema
        table_def = tab_api.TableDefinition(table)

        for column, column_type in zip(columns, column_types):
            table_def.add_column(column)

        if isinstance(table, tab_api.TableName) and table.schema_name:
            connection.catalog.create_schema_if_not_exists(table.schema_name)

        connection.catalog.create_table_if_not_exists(table_def)

    null_mask = np.ascontiguousarray(pd.isnull(df))
    # Special handling for conversions
    df, dtypes = _maybe_convert_timedelta(df)

    with tab_api.Inserter(connection, table_def) as inserter:
        libwriter.write_to_hyper(
            df.itertuples(index=False, name=None),
            null_mask,
            inserter._buffer,
            df.shape[1],
            dtypes,
        )
        inserter.execute()


def frame_to_hyper(
    df: pd.DataFrame,
    database: Union[str, pathlib.Path],
    *,
    table: pantab_types.TableType,
    table_mode: str = "w",
) -> None:
    """See api.rst for documentation"""
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

        if table_mode == "a" and pathlib.Path(database).exists():
            shutil.copy(database, tmp_db)

        with tab_api.Connection(
            hpe.endpoint, tmp_db, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            _insert_frame(df, connection=connection, table=table, table_mode=table_mode)

        # In Python 3.9+ we can just pass the path object, but due to bpo 32689
        # and subsequent typeshed changes it is easier to just pass as str for now
        shutil.move(str(tmp_db), database)


def frames_to_hyper(
    dict_of_frames: Dict[pantab_types.TableType, pd.DataFrame],
    database: Union[str, pathlib.Path],
    table_mode: str = "w",
) -> None:
    """See api.rst for documentation."""
    _validate_table_mode(table_mode)

    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hpe:
        tmp_db = pathlib.Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.hyper"

        if table_mode == "a" and pathlib.Path(database).exists():
            shutil.copy(database, tmp_db)

        with tab_api.Connection(
            hpe.endpoint, tmp_db, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            for table, df in dict_of_frames.items():
                _insert_frame(
                    df, connection=connection, table=table, table_mode=table_mode
                )

        # In Python 3.9+ we can just pass the path object, but due to bpo 32689
        # and subsequent typeshed changes it is easier to just pass as str for now
        shutil.move(str(tmp_db), database)
