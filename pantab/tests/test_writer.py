import re

import numpy as np
import pandas as pd
import pytest
import tableauhyperapi as tab_api

import pantab


def test_bad_table_mode_raises(df, tmp_hyper):
    msg = "'table_mode' must be either 'w' or 'a'"
    with pytest.raises(ValueError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test", table_mode="x")

    with pytest.raises(ValueError, match=msg):
        pantab.frames_to_hyper({"a": df}, tmp_hyper, table_mode="x")


def test_append_mode_raises_column_mismatch(df, tmp_hyper, table_name):
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name)

    df = df.drop("object", axis=1)
    msg = "^Mismatched column definitions:"
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode="a")


def test_append_mode_raises_column_dtype_mismatch(df, tmp_hyper, table_name):
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name)

    df["int16"] = df["int16"].astype(np.int64)
    msg = "^Mismatched column definitions:"
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode="a")


def test_failed_write_doesnt_overwrite_file(df, tmp_hyper, monkeypatch, table_mode):
    pantab.frame_to_hyper(df, tmp_hyper, table="test", table_mode=table_mode)
    last_modified = tmp_hyper.stat().st_mtime

    # Let's patch the Inserter to fail on creation
    def failure(*args, **kwargs):
        raise ValueError("dummy failure")

    monkeypatch.setattr(pantab._writer.tab_api, "Inserter", failure, raising=True)

    # Try out our write methods
    with pytest.raises(ValueError, match="dummy failure"):
        pantab.frame_to_hyper(df, tmp_hyper, table="test", table_mode=table_mode)
        pantab.frames_to_hyper({"test": df}, tmp_hyper, table_mode=table_mode)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_duplicate_columns_raises(tmp_hyper):
    df = pd.DataFrame([[1, 1]], columns=[1, 1])
    with pytest.raises(
        tab_api.hyperexception.HyperException,
        match="column '1' specified more than once",
    ):
        pantab.frame_to_hyper(df, tmp_hyper, table="foo")

    with pytest.raises(
        tab_api.hyperexception.HyperException,
        match="column '1' specified more than once",
    ):
        pantab.frames_to_hyper({"test": df}, tmp_hyper)


@pytest.mark.parametrize("dtype", ["UInt64", "datetime64[ns, US/Eastern]"])
def test_unsupported_dtype_raises(dtype, tmp_hyper):
    df = pd.DataFrame([[1]], dtype=dtype)

    msg = re.escape(f"Conversion of '{dtype}' dtypes not supported!")
    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test")


def test_bad_value_gives_clear_message(tmp_hyper):
    df = pd.DataFrame([[{"a": "b"}]], columns=["a"])

    msg = r"Invalid value \"{'a': 'b'}\" found \(row 0 column 0\)"

    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test")
