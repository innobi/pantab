from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as tm
import pytest
from tableauhyperapi import Connection, CreateMode, HyperProcess, TableName, Telemetry

import pantab
import pantab._compat as compat


def assert_roundtrip_equal(result, expected):
    """Compat helper for comparing round-tripped results."""

    if compat.PANDAS_100:
        expected["object"] = expected["object"].astype("string")
        expected["non-ascii"] = expected["non-ascii"].astype("string")

    tm.assert_frame_equal(result, expected)


def test_basic(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)
    expected["Float32"] = expected["Float32"].astype(np.float64)
    expected["Float64"] = expected["Float64"].astype(np.float64)

    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    assert_roundtrip_equal(result, expected)


def test_use_float_na_flag(df, tmp_hyper, table_name):
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name, use_float_na=False)
    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)
    expected["Float32"] = expected["Float32"].astype(np.float64)
    expected["Float64"] = expected["Float64"].astype(np.float64)
    assert_roundtrip_equal(result, expected)

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name, use_float_na=True)
    expected = df.copy()
    expected["float32"] = expected["float32"].astype("Float64")
    expected["float64"] = expected["float64"].astype("Float64")
    expected["float32_limits"] = expected["float32_limits"].astype("Float64")
    expected["float64_limits"] = expected["float64_limits"].astype("Float64")
    expected["Float32"] = expected["Float32"].astype("Float64")
    assert_roundtrip_equal(result, expected)


def test_multiple_tables(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)
    expected["Float32"] = expected["Float32"].astype(np.float64)
    expected["Float64"] = expected["Float64"].astype(np.float64)
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set((table_name, TableName("public", "table2")))
    for val in result.values():
        assert_roundtrip_equal(val, expected)


def test_roundtrip_with_external_hyper_process(df, tmp_hyper):
    default_log_path = Path.cwd() / "hyperd.log"
    if default_log_path.exists():
        default_log_path.unlink()

    # By passing in a pre-spawned HyperProcess, one can e.g. avoid creating a log file
    parameters = {"log_config": ""}
    with HyperProcess(
        Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=parameters
    ) as hyper:
        # test frame_to_hyper/frame_from_hyper
        pantab.frame_to_hyper(df, tmp_hyper, table="test", hyper_process=hyper)
        result = pantab.frame_from_hyper(tmp_hyper, table="test", hyper_process=hyper)
        expected = df.copy()
        expected["float32"] = expected["float32"].astype(np.float64)
        expected["Float32"] = expected["Float32"].astype(np.float64)
        expected["Float64"] = expected["Float64"].astype(np.float64)
        assert_roundtrip_equal(result, expected)

        # test frame_from_hyper_query
        result = pantab.frame_from_hyper_query(
            tmp_hyper, "SELECT * FROM test", hyper_process=hyper
        )
        assert result.size == df.size

        # test frames_to_hyper/frames_from_hyper
        pantab.frames_to_hyper(
            {"test2": df, "test": df}, tmp_hyper, hyper_process=hyper
        )
        result = pantab.frames_from_hyper(tmp_hyper, hyper_process=hyper)
        assert set(result.keys()) == set(
            (TableName("public", "test"), TableName("public", "test2"))
        )

        for val in result.values():
            assert_roundtrip_equal(val, expected)

    assert not default_log_path.exists()


def test_roundtrip_with_external_hyper_connection(df, tmp_hyper):
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        pantab.frames_to_hyper(
            {"test": df, "test2": df}, tmp_hyper, hyper_process=hyper
        )

        with Connection(hyper.endpoint, tmp_hyper, CreateMode.NONE) as connection:
            result = pantab.frame_from_hyper(connection, table="test")
            expected = df.copy()
            expected["float32"] = expected["float32"].astype(np.float64)
            expected["Float32"] = expected["Float32"].astype(np.float64)
            expected["Float64"] = expected["Float64"].astype(np.float64)
            assert_roundtrip_equal(result, expected)

            result = pantab.frame_from_hyper_query(connection, "SELECT * FROM test")
            assert result.size == df.size

            result = pantab.frames_from_hyper(connection)
            assert set(result.keys()) == set(
                (TableName("public", "test"), TableName("public", "test2"))
            )
            for val in result.values():
                assert_roundtrip_equal(val, expected)


def test_external_hyper_connection_and_process_error(df, tmp_hyper):
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, tmp_hyper, CreateMode.CREATE) as connection:
            expected_msg = (
                "hyper_process parameter is useless because `Connection` is provided"
            )
            with pytest.raises(ValueError, match=expected_msg):
                pantab.frame_from_hyper(connection, table="test", hyper_process=hyper)

            with pytest.raises(ValueError, match=expected_msg):
                pantab.frame_from_hyper_query(
                    connection, "SELECT * FROM test", hyper_process=hyper
                )

            with pytest.raises(ValueError, match=expected_msg):
                pantab.frames_from_hyper(connection, hyper_process=hyper)
