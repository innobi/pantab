import json

import pandas as pd
import pandas.testing as tm
import pytest
import tableauhyperapi as tab_api

import pantab


def test_read_doesnt_modify_existing_file(frame, tmp_hyper):
    pantab.frame_to_hyper(frame, tmp_hyper, table="test")
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pantab.frame_from_hyper(tmp_hyper, table="test")
    pantab.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_read_non_roundtrippable(datapath):
    result = pantab.frame_from_hyper(
        datapath / "dates.hyper", table=tab_api.TableName("Extract", "Extract")
    )
    expected = pd.DataFrame(
        [["1900-01-01", "2000-01-01"], [pd.NaT, "2050-01-01"]],
        columns=["Date1", "Date2"],
        dtype="date32[day][pyarrow]",
    )
    tm.assert_frame_equal(result, expected)


def test_reads_non_writeable(datapath):
    result = pantab.frame_from_hyper(
        datapath / "non_pantab_writeable.hyper",
        table=tab_api.TableName("public", "table"),
    )

    expected = pd.DataFrame(
        [["row1", 1.0], ["row2", 2.0]],
        columns=["Non-Nullable String", "Non-Nullable Float"],
    )
    expected["Non-Nullable Float"] = expected["Non-Nullable Float"].astype(
        "double[pyarrow]"
    )
    expected["Non-Nullable String"] = expected["Non-Nullable String"].astype(
        "large_string[pyarrow]"
    )

    tm.assert_frame_equal(result, expected)


def test_read_query(frame, tmp_hyper):
    pantab.frame_to_hyper(frame, tmp_hyper, table="test")

    query = "SELECT int16 AS i, '_' || int32 AS _i2 FROM test"
    result = pantab.frame_from_hyper_query(tmp_hyper, query)

    expected = pd.DataFrame([[1, "_2"], [6, "_7"], [0, "_0"]], columns=["i", "_i2"])
    expected = expected.astype({"i": "int16[pyarrow]", "_i2": "large_string[pyarrow]"})

    tm.assert_frame_equal(result, expected)


def test_empty_read_query(frame, roundtripped, tmp_hyper):
    """
    red-green for empty query results
    """
    # sql cols need to base case insensitive & unique
    table_name = "test"
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name)
    query = f"SELECT * FROM {table_name} limit 0"

    if not isinstance(frame, pd.DataFrame):
        pytest.skip("Need to implement this test properly for pyarrow")
    expected = pd.DataFrame(columns=frame.columns)
    expected = expected.astype(roundtripped.dtypes)

    result = pantab.frame_from_hyper_query(tmp_hyper, query)
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

    expected = pd.DataFrame(
        [["foo"], ["bar"]], columns=[column_name], dtype="large_string[pyarrow]"
    )

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    tm.assert_frame_equal(result, expected)


def test_read_oid(tmp_hyper):
    column_name = "OID Column"
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.oid(),
                nullability=tab_api.NOT_NULLABLE,
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
                inserter.add_rows([[123], [456]])
                inserter.execute()

    expected = pd.DataFrame(
        [[123], [456]], columns=[column_name], dtype="uint32[pyarrow]"
    )

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    tm.assert_frame_equal(result, expected)


def test_read_json(tmp_hyper):
    # Hyper just uses string to serialize/de-serialize, but we don't have an API
    # yet for users to control writing JSON. So just testing the read until then
    column_name = "JSON Column"
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.json(),
                nullability=tab_api.NOT_NULLABLE,
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
                inserter.add_rows(
                    [[json.dumps({"foo": 42})], [json.dumps({"bar": -42})]]
                )
                inserter.execute()

    expected = pd.DataFrame(
        [[json.dumps({"foo": 42})], [json.dumps({"bar": -42})]],
        columns=[column_name],
        dtype="large_string[pyarrow]",
    )

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    tm.assert_frame_equal(result, expected)


def test_read_geography(tmp_hyper):
    # Hyper uses a proprietary format for geography; best we can do is read out bytes
    column_name = "Geography Column"
    table_name = tab_api.TableName("public", "table")
    table = tab_api.TableDefinition(
        table_name=table_name,
        columns=[
            tab_api.TableDefinition.Column(
                name=column_name,
                type=tab_api.SqlType.geography(),
                nullability=tab_api.NOT_NULLABLE,
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

            inserter_definition = [
                tab_api.TableDefinition.Column(
                    name="geo_as_text",
                    type=tab_api.SqlType.text(),
                    nullability=tab_api.NOT_NULLABLE,
                )
            ]
            column_mappings = [
                tab_api.Inserter.ColumnMapping(
                    column_name, "CAST(geo_as_text AS GEOGRAPHY)"
                )
            ]
            with tab_api.Inserter(
                connection,
                table,
                column_mappings,
                inserter_definition=inserter_definition,
            ) as inserter:
                # WKT examples for Seattle / Munich taken from Hyper documentation
                # https://tableau.github.io/hyper-db/docs/guides/hyper_file/geodata
                inserter.add_rows(
                    [["point(-122.338083 47.647528)"], ["point(11.584329 48.139257)"]]
                )
                inserter.execute()

    expected = pd.DataFrame(
        [
            [
                b"\x07\xaa\x02\xe0%n\xd9\x01\x01\n\x00\xce\xab\xe8\xfa=\xff\x96\xf0\x8a\x9f\x01"
            ],
            [
                b"\x07\xaa\x02\x0c&n\x82\x01\x01\n\x00\xb0\xe2\xd4\xcc>\xd4\xbc\x97\x88\x0f"
            ],
        ],
        columns=[column_name],
        dtype="large_binary[pyarrow]",
    )

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    tm.assert_frame_equal(result, expected)
