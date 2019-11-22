import re

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
import tableauhyperapi as tab_api

import pantab


@pytest.fixture
def tmp_hyper(tmp_path):
    """A temporary file name to write / read a Hyper extract from."""
    return tmp_path / "test.hyper"


@pytest.fixture(params=["w", "a"])
def table_mode(request):
    """Write or append markers for table handling."""
    return request.param


@pytest.fixture(
    params=[
        "table",
        tab_api.Name("table"),
        tab_api.TableName("table"),
        tab_api.TableName("public", "table"),
        tab_api.TableName("nonpublic", "table"),
    ]
)
def table_name(request):
    """Various ways to represent a table in Tableau."""
    return request.param


def test_roundtrip(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = df.copy()

    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    expected["float32"] = expected["float32"].astype(np.float64)

    tm.assert_frame_equal(result, expected)


def test_roundtrip_missing_data(tmp_hyper, table_name, table_mode):
    df = pd.DataFrame([[np.nan], [1]], columns=list("a"))
    df["b"] = pd.Series([None, np.nan], dtype=object)  # no inference
    df["c"] = pd.Series([np.nan, "c"])

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    expected = pd.DataFrame(
        [[np.nan, np.nan, np.nan], [1, np.nan, "c"]], columns=list("abc")
    )
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    tm.assert_frame_equal(result, expected)


def test_roundtrip_multiple_tables(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = df.copy()
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    expected["float32"] = expected["float32"].astype(np.float64)

    # some test trickery here
    if not isinstance(table_name, tab_api.TableName) or table_name.schema_name is None:
        table_name = tab_api.TableName("public", table_name)

    assert set(result.keys()) == set(
        (table_name, tab_api.TableName("public", "table2"))
    )
    for val in result.values():
        tm.assert_frame_equal(val, expected)


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


def test_read_doesnt_modify_existing_file(df, tmp_hyper):
    pantab.frame_to_hyper(df, tmp_hyper, table="test")
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pantab.frame_from_hyper(tmp_hyper, table="test")
    pantab.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


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


def test_months_in_interval_raises(df, tmp_hyper, monkeypatch):
    # Monkeypatch a new constructor that hard codes months
    def __init__(self, months: int, days: int, microseconds: int):
        self.months = 1
        self.days = days
        self.microseconds = microseconds

    monkeypatch.setattr(pantab._writer.tab_api.Interval, "__init__", __init__)
    pantab.frame_to_hyper(df, tmp_hyper, table="test")
    with pytest.raises(
        ValueError, match=r"Cannot read Intervals with month componenets\."
    ):
        pantab.frame_from_hyper(tmp_hyper, table="test")

    with pytest.raises(
        ValueError, match=r"Cannot read Intervals with month componenets\."
    ):
        pantab.frames_from_hyper(tmp_hyper)


def test_bad_value_gives_clear_message(tmp_hyper):
    df = pd.DataFrame([[{"a": "b"}]], columns=["a"])

    msg = (
        r"Unsupported type '<class 'dict'>' for column type 'object' "
        r"\(column 'a' row 0\)"
    )

    with pytest.raises(TypeError, match=msg):
        pantab.frame_to_hyper(df, tmp_hyper, table="test")
