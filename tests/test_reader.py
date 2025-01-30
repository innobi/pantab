import pathlib

import pandas as pd
import pandas.testing as tm
import pytest

import pantab as pt


def test_read_doesnt_modify_existing_file(frame, tmp_hyper):
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        process_params={"default_database_version": "4"},
    )
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pt.frame_from_hyper(tmp_hyper, table="test")
    pt.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_reads_nullable_columns(tmp_hyper, compat):
    # We don't ever write nullable columns but we should be able to read them
    column_name = "int32"
    tab_api = pytest.importorskip("tableauhyperapi")
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

    result = pt.frame_from_hyper(tmp_hyper, table=table_name)
    expected = pd.DataFrame([[1], [2]], dtype="int32[pyarrow]", columns=[column_name])
    compat.assert_frame_equal(result, expected)


def test_read_query(frame, tmp_hyper):
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        process_params={"default_database_version": "4"},
    )

    query = "SELECT int16 AS i, '_' || int32 AS _i2 FROM test"
    result = pt.frame_from_hyper_query(tmp_hyper, query)

    expected = pd.DataFrame([[1, "_2"], [6, "_7"], [0, "_0"]], columns=["i", "_i2"])
    expected = expected.astype({"i": "int16[pyarrow]", "_i2": "large_string[pyarrow]"})

    tm.assert_frame_equal(result, expected)


def test_read_varchar(tmp_hyper):
    column_name = "VARCHAR Column"
    tab_api = pytest.importorskip("tableauhyperapi")
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.varchar(42),
                nullability=tab_api.NOT_NULLABLE,
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

    expected = pd.DataFrame(
        [["foo"], ["bar"]], columns=[column_name], dtype="large_string[pyarrow]"
    )

    result = pt.frame_from_hyper(tmp_hyper, table=table_name)
    tm.assert_frame_equal(result, expected)


def test_reader_handles_duplicate_columns(tmp_hyper):
    column_name = "does_not_matter"
    tab_api = pytest.importorskip("tableauhyperapi")
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.varchar(42),
                nullability=tab_api.NOT_NULLABLE,
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

    df = pt.frame_from_hyper_query(tmp_hyper, "SELECT 1 as col, 2 AS col, 3 AS col")
    assert df.columns.tolist() == ["col", "col_1", "col_2"]


def test_frame_from_hyper_doesnt_generate_hyperd_log(frame, tmp_hyper):
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        process_params={"default_database_version": "4"},
    )
    pt.frame_from_hyper(tmp_hyper, table="test")
    assert not pathlib.Path("hyperd.log").is_file()


def test_frames_from_hyper_doesnt_generate_hyperd_log(frame, tmp_hyper):
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        process_params={"default_database_version": "4"},
    )
    pt.frames_from_hyper(tmp_hyper)
    assert not pathlib.Path("hyperd.log").is_file()


def test_reader_accepts_process_params(tmp_hyper):
    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    pt.frame_to_hyper(frame, tmp_hyper, table="test")

    params = {"default_database_version": "0"}
    pt.frames_from_hyper(tmp_hyper, process_params=params)


@pytest.mark.skip_asan
def test_reader_invalid_process_params_raises(tmp_hyper):
    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    pt.frame_to_hyper(frame, tmp_hyper, table="test")

    params = {"not_a_real_parameter": "0"}
    msg = r"No internal setting named 'not_a_real_parameter'"
    with pytest.raises(RuntimeError, match=msg):
        pt.frames_from_hyper(tmp_hyper, process_params=params)


@pytest.mark.parametrize(
    "needs_hyperapi, hyperapi_obj, table_args",
    [
        (False, None, tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",))),
        (True, "Name", tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",))),
        (
            True,
            "TableName",
            tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",)),
        ),
        (
            True,
            "TableName",
            (
                "a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",
                "a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",
            ),
        ),
    ],
)
def test_reader_prevents_sql_injection(
    tmp_hyper, needs_hyperapi, hyperapi_obj, table_args
):
    if not needs_hyperapi:
        table = table_args[0]
    else:
        tab_api = pytest.importorskip("tableauhyperapi")
        table = getattr(tab_api, hyperapi_obj)(*table_args)

    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    pt.frame_to_hyper(frame, tmp_hyper, table=table)
    pt.frame_from_hyper(tmp_hyper, table=table)


def test_read_batches(tmp_hyper, compat):
    pa = pytest.importorskip("pyarrow")
    tbl = pa.table({"int": pa.array(range(4), type=pa.int16())})

    pt.frame_to_hyper(tbl, tmp_hyper, table="test")
    stream = pt.frame_from_hyper(
        tmp_hyper, table="test", return_type="stream", chunk_size=2
    )

    rdr = pa.RecordBatchReader.from_stream(stream)
    tbl1 = pa.table({"int": pa.array(range(2), type=pa.int16())})
    compat.assert_frame_equal(tbl1, pa.table(rdr.read_next_batch()))

    tbl2 = pa.table({"int": pa.array(range(2, 4), type=pa.int16())})
    compat.assert_frame_equal(tbl2, pa.table(rdr.read_next_batch()))

    with pytest.raises(StopIteration):
        rdr.read_next_batch()


@pytest.mark.parametrize("return_type", ["polars", "pandas", "pyarrow"])
def test_read_batches_without_capsule(tmp_hyper, compat, return_type):
    pa = pytest.importorskip("pyarrow")
    tbl = pa.table({"int": pa.array(range(4), type=pa.int16())})

    pt.frame_to_hyper(tbl, tmp_hyper, table="test")

    msg = r"only implemented with return_type='stream'"
    with pytest.raises(NotImplementedError, match=msg):
        pt.frame_from_hyper(
            tmp_hyper, table="test", return_type=return_type, chunk_size=2
        )


def test_reader_can_enable_logging(tmp_hyper):
    df = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    pt.frame_to_hyper(df, tmp_hyper, table="test")

    log_dir = tmp_hyper.parent
    params = {"log_config": "enable_me", "log_dir": str(log_dir)}
    pt.frame_from_hyper(tmp_hyper, table="test", process_params=params)

    assert (log_dir / "hyperd.log").exists()
    (log_dir / "hyperd.log").unlink()
