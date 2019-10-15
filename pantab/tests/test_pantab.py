import os
import pathlib
import re
import tempfile

from tableauhyperapi import TypeTag
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import pantab


@pytest.fixture
def df():
    df = pd.DataFrame(
        [
            [1, 2, 3, 4.0, 5.0, True, pd.to_datetime("1/1/18"), "foo"],
            [6, 7, 8, 9.0, 10.0, True, pd.to_datetime("1/1/19"), "foo"],
        ],
        columns=["foo", "bar", "baz", "qux", "quux", "quuuz", "corge", "garply"],
    )

    df = df.astype(
        {
            "foo": np.int16,
            "bar": np.int32,
            "baz": np.int64,
            "qux": np.float32,
            "quux": np.float64,
            "quuuz": np.bool,
            "corge": "datetime64[ns]",
            # 'grault': 'timedelta64[ns]',
            "garply": "object",
        }
    )

    return df


def test_roundtrip(self, tmp_path):
    fn = tmp_path / "test.hyper"
    table_name = "some_table"

    df = pd.DataFrame(
        [
            [1, 2, 3, 4.0, 5.0, True, pd.to_datetime("1/1/18"), "foo"],
            [6, 7, 8, 9.0, 10.0, True, pd.to_datetime("1/1/19"), "foo"],
        ],
        columns=["foo", "bar", "baz", "qux", "quux", "quuuz", "corge", "garply"],
    )

    pantab.frame_to_hyper(df, fn, table=table_name)
    result = pantab.frame_from_hyper(fn, table=table_name)
    expected = df

    tm.assert_frame_equal(result, expected)

def test_roundtrip_missing_data(self, tmp_path):
    fn = tmp_path / "test.hyper"
    table_name = "some_table"

    df = pd.DataFrame([[np.nan], [1]], columns=list("a"))
    df["b"] = pd.Series([None, np.nan], dtype=object)  # no inference
    df["c"] = pd.Series([np.nan, "c"])

    pantab.frame_to_hyper(df, fn, table=table_name)

    result = pantab.frame_from_hyper(fn, table=table_name)
    expected = pd.DataFrame(
        [[np.nan, np.nan, np.nan], [1, np.nan, "c"]], columns=list("abc")
    )
    tm.assert_frame_equal(result, expected)

def test_roundtrip_schema(self, tmp_path):
    fn = tmp_path / "test.hyper"
    table_name = "some_table"
    schema = "a_schema"

    df = pd.DataFrame(
        [
            [1, 2, 3, 4.0, 5.0, True, pd.to_datetime("1/1/18"), "foo"],
            [6, 7, 8, 9.0, 10.0, True, pd.to_datetime("1/1/19"), "foo"],
        ],
        columns=["foo", "bar", "baz", "qux", "quux", "quuuz", "corge", "garply"],
    )

    pantab.frame_to_hyper(df, fn, schema=schema, table=table_name)
    result = pantab.frame_from_hyper(fn, schema=schema, table=table_name)
    expected = df

    tm.assert_frame_equal(result, expected)
