import datetime
import re

import narwhals as nw
import pandas as pd
import pyarrow as pa
import pytest
import tableauhyperapi as tab_api

import pantab as pt


def test_bad_table_mode_raises(frame, tmp_hyper):
    msg = "'table_mode' must be either 'w' or 'a'"
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(
            frame,
            tmp_hyper,
            table="test",
            table_mode="x",
        )

    with pytest.raises(ValueError, match=msg):
        pt.frames_to_hyper({"a": frame}, tmp_hyper, table_mode="x")


@pytest.mark.parametrize(
    "new_dtype,hyper_type_name",
    [(nw.Int64, "BIGINT"), (nw.Float64, "DOUBLE PRECISION")],
)
def test_append_mode_raises_column_dtype_mismatch(
    new_dtype, hyper_type_name, frame, tmp_hyper, table_name, compat
):
    frame = compat.select_columns(frame, ["int16"])
    pt.frame_to_hyper(frame, tmp_hyper, table=table_name)

    frame = compat.cast_column_to_type(frame, "int16", new_dtype)
    msg = f"Column type mismatch at index 0; new: {hyper_type_name} old: SMALLINT"
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_append_mode_raises_ncolumns_mismatch(frame, tmp_hyper, table_name, compat):
    pt.frame_to_hyper(frame, tmp_hyper, table=table_name)

    frame = compat.drop_columns(frame, ["int16"])
    msg = "Number of columns"
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


@pytest.mark.parametrize("container_t", [set, list, tuple])
def test_writer_creates_not_null_columns(tmp_hyper, container_t):
    table_name = tab_api.TableName("test")
    df = pd.DataFrame({"int32": [1, 2, 3]}, dtype="int32")
    pt.frame_to_hyper(
        df,
        tmp_hyper,
        table=table_name,
        table_mode="a",
        not_null_columns=container_t(("int32",)),
    )

    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(table_name)
            col = table_def.get_column_by_name("int32")
            assert col.nullability == tab_api.Nullability.NOT_NULLABLE


@pytest.mark.parametrize("container_t", [set, list, tuple])
def test_writing_to_non_nullable_column_without_nulls(tmp_hyper, container_t):
    # With arrow as our backend we define everything as nullable, so it is up
    # to the users to override this if they want
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
        telemetry=tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
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

    df = pd.DataFrame({"int32": [1, 2, 3]}, dtype="int32")
    pt.frame_to_hyper(
        df,
        tmp_hyper,
        table=table_name,
        table_mode="a",
        not_null_columns=container_t(("int32",)),
    )


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
        telemetry=tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
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
    pt.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_failed_write_doesnt_overwrite_file(
    frame, tmp_hyper, monkeypatch, table_mode, compat
):
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        table_mode=table_mode,
    )
    last_modified = tmp_hyper.stat().st_mtime

    frame = compat.add_non_writeable_column(frame)
    msg = "Unsupported Arrow type"
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table="test", table_mode=table_mode)
        pt.frames_to_hyper({"test": frame}, tmp_hyper, table_mode=table_mode)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_duplicate_columns_raises(tmp_hyper):
    frame = pd.DataFrame([[1, 1]], columns=[1, 1])
    msg = r"Duplicate column names found: \[1, 1\]"
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table="foo")

    with pytest.raises(ValueError, match=msg):
        pt.frames_to_hyper({"test": frame}, tmp_hyper)


def test_unsupported_dtype_raises(tmp_hyper):
    frame = pd.DataFrame([[pd.Timedelta("1D")]])

    msg = re.escape("Unsupported Arrow type: duration")
    with pytest.raises(ValueError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table="test")


def test_utc_bug(tmp_hyper):
    """
    Red-Green for UTC bug
    """
    frame = pd.DataFrame(
        {
            "utc_time": [
                datetime.datetime.now(datetime.timezone.utc),
                pd.Timestamp("today", tz="UTC"),
            ]
        }
    )
    pt.frame_to_hyper(frame, tmp_hyper, table="exp")
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
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


def test_uint32_actually_writes_as_oid(tmp_hyper, frame):
    pt.frame_to_hyper(frame, tmp_hyper, table="test")
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(
                tab_api.TableName("test")
            )
            oid_col = table_def.get_column_by_name("oid")
            assert oid_col.type == tab_api.SqlType.oid()


@pytest.mark.parametrize("container_t", [set, list, tuple])
def test_geo_and_json_columns_writes_proper_type(tmp_hyper, frame, container_t):
    pt.frame_to_hyper(frame, tmp_hyper, table="test")

    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(
                tab_api.TableName("test")
            )
            json_col = table_def.get_column_by_name("json")
            geo_col = table_def.get_column_by_name("geography")
            assert json_col.type == tab_api.SqlType.text()
            assert geo_col.type == tab_api.SqlType.bytes()

    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        json_columns=container_t(("json",)),
        geo_columns=container_t(("geography",)),
    )

    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(
                tab_api.TableName("test")
            )
            json_col = table_def.get_column_by_name("json")
            geo_col = table_def.get_column_by_name("geography")
            assert json_col.type == tab_api.SqlType.json()
            assert geo_col.type == tab_api.SqlType.geography()


def test_can_write_wkt_as_geo(tmp_hyper):
    df = pd.DataFrame(
        [
            ["point(-122.338083 47.647528)"],
            ["point(11.584329 48.139257)"],
        ],
        columns=["geography"],
    )

    pt.frame_to_hyper(df, tmp_hyper, table="test", geo_columns=["geography"])
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(
                tab_api.TableName("test")
            )
            geo_col = table_def.get_column_by_name("geography")
            assert geo_col.type == tab_api.SqlType.geography()
            data = connection.execute_list_query("select * from test")

    assert data[0][0] == (
        b"\x07\xaa\x02\xe0%n\xd9\x01\x01\n\x00\xce\xab\xe8\xfa=\xff\x96\xf0\x8a\x9f\x01"
    )
    assert data[1][0] == (
        b"\x07\xaa\x02\x0c&n\x82\x01\x01\n\x00\xb0\xe2\xd4\xcc>\xd4\xbc\x97\x88\x0f"
    )


def test_can_write_chunked_frames(chunked_frame, tmp_hyper):
    pt.frame_to_hyper(chunked_frame, tmp_hyper, table="test")
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            data = connection.execute_list_query("select * from test")

    assert data == [[1], [2], [3], [4], [5], [6]]


def test_write_date_bug(tmp_hyper):
    # GH282
    schema = pa.schema([("date32_test_col", pa.date32())])

    tbl = pa.Table.from_arrays(
        [
            pa.array(
                [
                    datetime.date(2024, 2, 1),
                    datetime.date(2024, 3, 16),
                    datetime.date(2024, 3, 1),
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 31),
                ]
            )
        ],
        schema=schema,
    )

    pt.frame_to_hyper(tbl, tmp_hyper, table="test")
    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            data = connection.execute_list_query("select * from test")

    assert data == [
        [tab_api.Date(2024, 2, 1)],
        [tab_api.Date(2024, 3, 16)],
        [tab_api.Date(2024, 3, 1)],
        [tab_api.Date(2024, 1, 1)],
        [tab_api.Date(2024, 1, 31)],
    ]


def test_eight_bit_int(tmp_hyper):
    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")

    pt.frame_to_hyper(frame, tmp_hyper, table="test")

    with tab_api.HyperProcess(
        tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        parameters={"log_config": ""},
    ) as hyper:
        with tab_api.Connection(
            hyper.endpoint, tmp_hyper, tab_api.CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            table_def = connection.catalog.get_table_definition(
                tab_api.TableName("test")
            )
            num_col = table_def.get_column_by_name("nums")
            assert num_col is not None
            assert num_col.type == tab_api.SqlType.small_int()


def test_writer_accepts_process_params(tmp_hyper):
    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    params = {"default_database_version": "0"}
    pt.frame_to_hyper(frame, tmp_hyper, table="test", process_params=params)


def test_writer_invalid_process_params_raises(tmp_hyper):
    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    params = {"not_a_real_parameter": "0"}

    msg = r"No internal setting named 'not_a_real_parameter'"
    with pytest.raises(RuntimeError, match=msg):
        pt.frame_to_hyper(frame, tmp_hyper, table="test", process_params=params)
