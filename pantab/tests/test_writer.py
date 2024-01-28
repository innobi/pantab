import re
from datetime import datetime, timezone

import pandas as pd
import pytest
import tableauhyperapi as tab_api
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    SqlType,
    TableName,
    Telemetry,
)

import pantab


def test_bad_table_mode_raises(frame, tmp_hyper):
    msg = "'table_mode' must be either 'w' or 'a'"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(
            frame,
            tmp_hyper,
            table="test",
            table_mode="x",
        )

    with pytest.raises(ValueError, match=msg):
        pantab.frames_to_hyper({"a": frame}, tmp_hyper, table_mode="x")


@pytest.mark.parametrize(
    "new_dtype,hyper_type_name", [("int64", "BIGINT"), ("float", "DOUBLE PRECISION")]
)
def test_append_mode_raises_column_dtype_mismatch(
    new_dtype, hyper_type_name, frame, tmp_hyper, table_name, compat
):
    frame = compat.select_columns(frame, ["int16"])
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name)

    frame = compat.cast_column_to_type(frame, "int16", new_dtype)
    msg = f"Column type mismatch at index 0; new: {hyper_type_name} old: SMALLINT"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_append_mode_raises_ncolumns_mismatch(frame, tmp_hyper, table_name, compat):
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name)

    frame = compat.drop_columns(frame, ["int16"])
    msg = "Number of columns"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


@pytest.mark.skip("Hyper API calls abort() when this condition is not met")
def test_writing_to_non_nullable_column_without_nulls(frame, tmp_hyper, compat):
    # With arrow as our backend we define everything as nullable, but we should
    # still be able to append to non-nullable columns
    column_name = "int32"
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.int(),
                nullability=tab_api.NULLABLE,
            )
        ],
    )

    with tab_api.HyperProcess(
        telemetry=tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hyper:
        with tab_api.Connection(
            endpoint=hyper.endpoint,
            database=tmp_hyper,
            create_mode=tab_api.CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_table(table_definition=table)

            with tab_api.Inserter(connection, table) as inserter:
                inserter.add_rows([[1], [2]])
                inserter.execute()

    frame = compat.select_columns(frame, [column_name])
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_string_type_to_existing_varchar(frame, tmp_hyper, compat):
    column_name = "string"
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.varchar(42),
                nullability=tab_api.NULLABLE,
            )
        ],
    )

    with tab_api.HyperProcess(
        telemetry=tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hyper:
        with tab_api.Connection(
            endpoint=hyper.endpoint,
            database=tmp_hyper,
            create_mode=tab_api.CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_table(table_definition=table)

            with tab_api.Inserter(connection, table) as inserter:
                inserter.add_rows([["foo"], ["bar"]])
                inserter.execute()

    frame = compat.select_columns(frame, [column_name])
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_failed_write_doesnt_overwrite_file(
    frame, tmp_hyper, monkeypatch, table_mode, compat
):
    pantab.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        table_mode=table_mode,
    )
    last_modified = tmp_hyper.stat().st_mtime

    frame = compat.add_non_writeable_column(frame)
    msg = "Unsupported Arrow type"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table="test", table_mode=table_mode)
        pantab.frames_to_hyper({"test": frame}, tmp_hyper, table_mode=table_mode)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_duplicate_columns_raises(tmp_hyper):
    frame = pd.DataFrame([[1, 1]], columns=[1, 1])
    msg = r"Duplicate column names found: \[1, 1\]"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table="foo")

    with pytest.raises(ValueError, match=msg):
        pantab.frames_to_hyper({"test": frame}, tmp_hyper)


def test_unsupported_dtype_raises(tmp_hyper):
    frame = pd.DataFrame([[pd.Timedelta("1D")]])

    msg = re.escape("Unsupported Arrow type")
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table="test")


def test_utc_bug(tmp_hyper):
    """
    Red-Green for UTC bug
    """
    frame = pd.DataFrame(
        {"utc_time": [datetime.now(timezone.utc), pd.Timestamp("today", tz="UTC")]}
    )
    pantab.frame_to_hyper(frame, tmp_hyper, table="exp")
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint, tmp_hyper, CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            resp = connection.execute_list_query("select utc_time from exp")
    assert all(
        [
            actual[0].year == expected.year
            for actual, expected in zip(resp, frame.utc_time)
        ]
    ), f"""
    expected: {frame.utc_time}
    actual: {[c[0] for c in resp]}
    """


def test_geo_and_json_columns_writes_proper_type(tmp_hyper, frame):
    pantab.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
    )

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint, tmp_hyper, CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(TableName("test"))
            json_col = table_def.get_column_by_name("json")
            geo_col = table_def.get_column_by_name("geography")
            assert json_col.type == SqlType.text()
            assert geo_col.type == SqlType.bytes()

    pantab.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        json_columns={"json"},
        geo_columns={"geography"},
    )

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint, tmp_hyper, CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(TableName("test"))
            json_col = table_def.get_column_by_name("json")
            geo_col = table_def.get_column_by_name("geography")
            assert json_col.type == SqlType.json()
            assert geo_col.type == SqlType.geography()
