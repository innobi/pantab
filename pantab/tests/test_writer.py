import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest
import tableauhyperapi as tab_api
from tableauhyperapi import Connection, CreateMode, HyperProcess, Telemetry

import pantab


def test_bad_table_mode_raises(df, tmp_hyper, use_parquet):
    if use_parquet:
        pytest.importorskip("pyarrow")
        df = df.drop(columns=["timedelta64"])

    msg = "'table_mode' must be either 'w' or 'a'"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(
            df, tmp_hyper, table="test", table_mode="x", use_parquet=use_parquet
        )

    with pytest.raises(ValueError, match=msg):
        pantab.frames_to_hyper({"a": df}, tmp_hyper, table_mode="x")


def test_append_mode_raises_column_mismatch(df, tmp_hyper, table_name, use_parquet):
    if use_parquet:
        pytest.importorskip("pyarrow")
        df = df.drop(columns=["timedelta64"])

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, use_parquet=use_parquet)

    df = df.drop("object", axis=1)
    msg = "^Mismatched column definitions:"
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(
            df, tmp_hyper, table=table_name, table_mode="a", use_parquet=use_parquet
        )


def test_append_mode_raises_column_dtype_mismatch(
    df, tmp_hyper, table_name, use_parquet
):
    if use_parquet:
        pytest.importorskip("pyarrow")
        df = df.drop(columns=["timedelta64"])

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, use_parquet=use_parquet)

    df["int16"] = df["int16"].astype(np.int64)
    msg = "^Mismatched column definitions:"
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(
            df, tmp_hyper, table=table_name, table_mode="a", use_parquet=use_parquet
        )


def test_failed_write_doesnt_overwrite_file(
    df, tmp_hyper, monkeypatch, table_mode, use_parquet
):
    if use_parquet:
        pytest.importorskip("pyarrow")
        df = df.drop(columns=["timedelta64"])

    pantab.frame_to_hyper(
        df, tmp_hyper, table="test", table_mode=table_mode, use_parquet=use_parquet
    )
    last_modified = tmp_hyper.stat().st_mtime

    # Let's patch the Inserter to fail on creation
    def failure(*args, **kwargs):
        raise ValueError("dummy failure")

    if use_parquet:
        pytest.importorskip("pyarrow")
        pytest.skip("TODO: should figure out patching here")
        # monkeypatch.setattr(pantab._writer.pq, "write_table", failure, raising=True)
    else:
        monkeypatch.setattr(pantab._writer.tab_api, "Inserter", failure, raising=True)

    # Try out our write methods
    with pytest.raises(ValueError, match="dummy failure"):
        pantab.frame_to_hyper(
            df, tmp_hyper, table="test", table_mode=table_mode, use_parquet=use_parquet
        )
        pantab.frames_to_hyper(
            {"test": df}, tmp_hyper, table_mode=table_mode, use_parquet=use_parquet
        )

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_duplicate_columns_raises(tmp_hyper, use_parquet):
    df = pd.DataFrame([[1, 1]], columns=[1, 1])
    with pytest.raises(
        tab_api.hyperexception.HyperException,
        match="column '1' specified more than once",
    ):
        pantab.frame_to_hyper(df, tmp_hyper, table="foo", use_parquet=use_parquet)

    with pytest.raises(
        tab_api.hyperexception.HyperException,
        match="column '1' specified more than once",
    ):
        pantab.frames_to_hyper({"test": df}, tmp_hyper, use_parquet=use_parquet)


@pytest.mark.parametrize("dtype", ["UInt64", "datetime64[ns, US/Eastern]"])
def test_unsupported_dtype_raises(dtype, tmp_hyper, use_parquet):
    df = pd.DataFrame([[1]], dtype=dtype)

    msg = re.escape(f"Conversion of '{dtype}' dtypes not supported!")
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test", use_parquet=use_parquet)


def test_bad_value_gives_clear_message(tmp_hyper):
    df = pd.DataFrame([[{"a": "b"}]], columns=["a"])

    msg = r"Invalid value \"{'a': 'b'}\" found \(row 0 column 0\)"

    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test")


def test_use_parquet_with_timedelta_raises(df, tmp_hyper):
    msg = "Writing timedelta values with use_parquet=True is not yet supported."
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test", use_parquet=True)


@pytest.mark.skipif(
    not pantab._compat.PANDAS_130, reason="bug is specifically with >=pandas 1.3"
)
def test_utc_bug(tmp_hyper):
    """
    Red-Green for UTC bug
    """
    df = pd.DataFrame(
        {"utc_time": [datetime.now(timezone.utc), pd.Timestamp("today", tz="UTC")]}
    )
    pantab.frame_to_hyper(df, tmp_hyper, table="exp")
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint, tmp_hyper, CreateMode.CREATE_IF_NOT_EXISTS
        ) as connection:
            resp = connection.execute_list_query("select utc_time from exp")
    assert all(
        [actual[0].year == expected.year for actual, expected in zip(resp, df.utc_time)]
    ), f"""
    expected: {df.utc_time}
    actual: {[c[0] for c in resp]}
    """


@pytest.mark.skipif(
    not pantab._compat.PANDAS_130, reason="bug is specifically with >=pandas 1.3"
)
def test_maybe_convert_utc(tmp_hyper):
    """
    timezone aware is not supported, thus we ensure timezone naive
    """
    df = pd.DataFrame(
        {"utc_time": [datetime.now(timezone.utc), pd.Timestamp("today", tz="UTC")]}
    )
    assert not df.select_dtypes("datetime64[ns, UTC]").empty
    df = pantab._writer._maybe_convert_utctimestamp(df)
    assert df.select_dtypes("datetime64[ns, UTC]").empty
    assert not df.select_dtypes("datetime64[ns]").empty
