import re
from datetime import datetime, timezone

import pandas as pd
import pytest
from tableauhyperapi import Connection, CreateMode, HyperProcess, Telemetry

import pantab
import pantab.tests.util as tu


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
    new_dtype, hyper_type_name, frame, tmp_hyper, table_name
):
    frame = tu.select_columns(frame, "int16")
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name)

    tu.cast_column_to_type(frame, "int16", new_dtype)
    msg = f"Column type mismatch at index 0; new: {hyper_type_name} old: SMALLINT"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_append_mode_raises_ncolumns_mismatch(frame, tmp_hyper, table_name):
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name)

    frame = tu.drop_columns(frame, ["int16"])
    msg = "Number of columns"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode="a")


def test_failed_write_doesnt_overwrite_file(frame, tmp_hyper, monkeypatch, table_mode):
    pantab.frame_to_hyper(
        frame,
        tmp_hyper,
        table="test",
        table_mode=table_mode,
    )
    last_modified = tmp_hyper.stat().st_mtime

    # Pick a dtype we know will fail
    frame = tu.add_non_writeable_column(frame)
    # Try out our write methods
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
