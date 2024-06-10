import pathlib

import pandas as pd
import pandas.testing as tm
import tableauhyperapi as tab_api

import pantab as pt


def test_read_doesnt_modify_existing_file(frame, tmp_hyper):
    pt.frame_to_hyper(frame, tmp_hyper, table="test")
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pt.frame_from_hyper(tmp_hyper, table="test")
    pt.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_reads_nullable_columns(tmp_hyper, compat):
    # We don't ever write nullable columns but we should be able to read them
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
    pt.frame_to_hyper(frame, tmp_hyper, table="test")

    query = "SELECT int16 AS i, '_' || int32 AS _i2 FROM test"
    result = pt.frame_from_hyper_query(tmp_hyper, query)

    expected = pd.DataFrame([[1, "_2"], [6, "_7"], [0, "_0"]], columns=["i", "_i2"])
    expected = expected.astype({"i": "int16[pyarrow]", "_i2": "large_string[pyarrow]"})

    tm.assert_frame_equal(result, expected)


def test_read_varchar(tmp_hyper):
    column_name = "VARCHAR Column"
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
    pt.frame_to_hyper(frame, tmp_hyper, table="test")
    pt.frame_from_hyper(tmp_hyper, table="test")
    assert not pathlib.Path("hyperd.log").is_file()


def test_frames_from_hyper_doesnt_generate_hyperd_log(frame, tmp_hyper):
    pt.frame_to_hyper(frame, tmp_hyper, table="test")
    pt.frames_from_hyper(tmp_hyper)
    assert not pathlib.Path("hyperd.log").is_file()
