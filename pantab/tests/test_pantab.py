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
    """Fixture to use which should contain all data types."""
    df = pd.DataFrame(
        [
            [1, 2, 3, 4.0, 5.0, True, pd.to_datetime("1/1/18"), "foo"],
            [6, 7, 8, 9.0, 10.0, True, pd.to_datetime("1/1/19"), "foo"],
        ],
        columns=["int16", "int32", "int64", "float32", "float64", "bool", "datetime64", "object"],
    )

    df = df.astype(
        {
            "int16": np.int16,
            "int32": np.int32,
            "int64": np.int64,
            "float32": np.float32,
            "float64": np.float64,
            "bool": np.bool,
            "datetime64": "datetime64[ns]",
            # 'grault': 'timedelta64[ns]',
            "object": "object",
        }
    )

    return df


@pytest.fixture(params=[None, "schema"])
def schema(request):
    """Domain values for schema argument."""
    return request.param


@pytest.fixture
def tmp_hyper(tmp_path):
    """A temporary file name to write / read a Hyper extract from."""
    return tmp_path / "test.hyper"


def test_roundtrip(df, tmp_hyper, schema):
    table_name = "some_table"

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, schema=schema)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name, schema=schema)
    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)

    tm.assert_frame_equal(result, expected)


def test_roundtrip_missing_data(tmp_hyper, schema):
    table_name = "some_table"

    df = pd.DataFrame([[np.nan], [1]], columns=list("a"))
    df["b"] = pd.Series([None, np.nan], dtype=object)  # no inference
    df["c"] = pd.Series([np.nan, "c"])

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, schema=schema)

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name, schema=schema)
    expected = pd.DataFrame(
        [[np.nan, np.nan, np.nan], [1, np.nan, "c"]], columns=list("abc")
    )
    tm.assert_frame_equal(result, expected)


def test_roundtrip_multiple_tables(df, tmp_hyper, schema):
    pantab.frames_to_hyper({
        "table1": df,
        "table2": df,
    }, tmp_hyper, schema=schema)

    result = pantab.frames_from_hyper(tmp_hyper, tables=["table1", "table2"], schema=schema)
    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)    

    assert result.keys() == ("table1", "table2")
    for val in result.values():
        tm.assert_frame_equal(val, expected)
    
    
