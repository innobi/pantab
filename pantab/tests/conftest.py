import pathlib

import numpy as np
import pandas as pd
import pytest
import tableauhyperapi as tab_api

import pantab._compat as compat


@pytest.fixture
def df():
    """Fixture to use which should contain all data types."""
    df = pd.DataFrame(
        [
            [
                1,
                2,
                3,
                1,
                2,
                3,
                4.0,
                5.0,
                True,
                pd.to_datetime("2018-01-01"),
                pd.to_datetime("2018-01-01", utc=True),
                pd.Timedelta("1 days 2 hours 3 minutes 4 seconds"),
                "foo",
                np.iinfo(np.int16).min,
                np.iinfo(np.int32).min,
                np.iinfo(np.int64).min,
                -(2 ** 24),
                -(2 ** 53),
                "\xef\xff\xdc\xde\xee",
            ],
            [
                6,
                7,
                8,
                np.nan,
                np.nan,
                np.nan,
                9.0,
                10.0,
                False,
                pd.to_datetime("1/1/19"),
                pd.to_datetime("2019-01-01", utc=True),
                pd.Timedelta("-1 days 2 hours 3 minutes 4 seconds"),
                "bar",
                np.iinfo(np.int16).max,
                np.iinfo(np.int32).max,
                np.iinfo(np.int64).max,
                2 ** 24 - 1,
                2 ** 53 - 1,
                "\xfa\xfb\xdd\xaf\xaa",
            ],
            [
                0,
                0,
                0,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                False,
                pd.NaT,
                pd.NaT,
                pd.NaT,
                np.nan,
                0,
                0,
                0,
                np.nan,
                np.nan,
                np.nan,
            ],
        ],
        columns=[
            "int16",
            "int32",
            "int64",
            "Int16",
            "Int32",
            "Int64",
            "float32",
            "float64",
            "bool",
            "datetime64",
            "datetime64_utc",
            "timedelta64",
            "object",
            "int16_limits",
            "int32_limits",
            "int64_limits",
            "float32_limits",
            "float64_limits",
            "non-ascii",
        ],
    )

    df = df.astype(
        {
            "int16": np.int16,
            "Int16": "Int16",
            "int32": np.int32,
            "Int32": "Int32",
            "int64": np.int64,
            "Int32": "Int32",
            "float32": np.float32,
            "float64": np.float64,
            "bool": np.bool,
            "datetime64": "datetime64[ns]",
            "datetime64_utc": "datetime64[ns, UTC]",
            "timedelta64": "timedelta64[ns]",
            "object": "object",
            "int16_limits": np.int16,
            "int32_limits": np.int32,
            "int64_limits": np.int64,
            "float32_limits": np.float64,
            "float64_limits": np.float64,
            "non-ascii": "object",
        }
    )

    if compat.PANDAS_100:
        df["boolean"] = pd.Series([True, False, pd.NA], dtype="boolean")
        df["string"] = pd.Series(["foo", "bar", pd.NA], dtype="string")

    return df


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


@pytest.fixture
def datapath():
    """Location of data files in test folder."""
    return pathlib.Path(__file__).parent / "data"
