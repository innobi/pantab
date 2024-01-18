import pathlib

import numpy as np
import pandas as pd
import pytest
import tableauhyperapi as tab_api


def get_basic_dataframe():
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
                1.0,
                2.0,
                True,
                True,
                pd.to_datetime("2024-01-01"),
                pd.to_datetime("2018-01-01"),
                pd.to_datetime("2018-01-01", utc=True),
                "foo",
                "foo",
                np.iinfo(np.int16).min,
                np.iinfo(np.int32).min,
                np.iinfo(np.int64).min,
                -(2**24),
                -(2**53),
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
                1.0,
                2.0,
                False,
                False,
                pd.to_datetime("2024-01-01"),
                pd.to_datetime("1/1/19"),
                pd.to_datetime("2019-01-01", utc=True),
                "bar",
                "bar",
                np.iinfo(np.int16).max,
                np.iinfo(np.int32).max,
                np.iinfo(np.int64).max,
                2**24 - 1,
                2**53 - 1,
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
                pd.NA,
                pd.NA,
                False,
                pd.NA,
                pd.NaT,
                pd.NaT,
                pd.NaT,
                np.nan,
                pd.NA,
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
            "Float32",
            "Float64",
            "bool",
            "boolean",
            "date32",
            "datetime64",
            "datetime64_utc",
            "object",
            "string",
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
            "Int64": "Int64",
            "float32": np.float32,
            "float64": np.float64,
            "Float32": "Float32",
            "Float64": "Float64",
            "bool": bool,
            "boolean": "boolean",
            "date32": "date32[pyarrow]",
            "datetime64": "datetime64[ns]",
            "datetime64_utc": "datetime64[ns, UTC]",
            "object": "object",
            "string": "string",
            "int16_limits": np.int16,
            "int32_limits": np.int32,
            "int64_limits": np.int64,
            "float32_limits": np.float64,
            "float64_limits": np.float64,
            "non-ascii": "object",
        }
    )

    return df


@pytest.fixture
def df():
    """Fixture to use which should contain all data types."""
    return get_basic_dataframe()


@pytest.fixture
def roundtripped():
    """Roundtripped DataFrames should use arrow dtypes by default"""
    df = get_basic_dataframe()
    df = df.astype(
        {
            "int16": "int16[pyarrow]",
            "int32": "int32[pyarrow]",
            "int64": "int64[pyarrow]",
            "Int16": "int16[pyarrow]",
            "Int32": "int32[pyarrow]",
            "Int64": "int64[pyarrow]",
            "float32": "double[pyarrow]",
            "float64": "double[pyarrow]",
            "Float32": "double[pyarrow]",
            "Float64": "double[pyarrow]",
            "bool": "boolean[pyarrow]",
            "boolean": "boolean[pyarrow]",
            "datetime64": "timestamp[us][pyarrow]",
            "datetime64_utc": "timestamp[us, UTC][pyarrow]",
            # "timedelta64": "timedelta64[ns]",
            "object": "large_string[pyarrow]",
            "int16_limits": "int16[pyarrow]",
            "int32_limits": "int32[pyarrow]",
            "int64_limits": "int64[pyarrow]",
            "float32_limits": "double[pyarrow]",
            "float64_limits": "double[pyarrow]",
            "non-ascii": "large_string[pyarrow]",
            "string": "large_string[pyarrow]",
        }
    )
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
