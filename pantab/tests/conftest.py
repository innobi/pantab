import datetime
import pathlib

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import tableauhyperapi as tab_api


def basic_arrow_table():
    tbl = pa.Table.from_arrays(
        [
            pa.array([1, 6, 0], type=pa.int16()),
            pa.array([2, 7, 0], type=pa.int32()),
            pa.array([3, 8, 0], type=pa.int64()),
            pa.array([1, None, None], type=pa.int16()),
            pa.array([2, None, None], type=pa.int32()),
            pa.array([3, None, None], type=pa.int64()),
            pa.array([4, 9.0, None], type=pa.float32()),
            pa.array([5, 10.0, None], type=pa.float64()),
            pa.array([1.0, 1.0, None], type=pa.float32()),
            pa.array([2.0, 2.0, None], type=pa.float64()),
            pa.array([True, False, False], type=pa.bool_()),
            pa.array([True, False, None], type=pa.bool_()),
            pa.array(
                [datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), None],
                type=pa.date32(),
            ),
            pa.array(
                [
                    datetime.datetime(2018, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    None,
                ],
                type=pa.timestamp("us"),
            ),
            pa.array(
                [
                    datetime.datetime(2018, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    None,
                ],
                type=pa.timestamp("us", "utc"),
            ),
            pa.array(["foo", "bar", None], type=pa.large_string()),
            pa.array(["foo", "bar", None], type=pa.string()),
            pa.array([-(2**15), 2**15 - 1, 0], type=pa.int16()),
            pa.array([-(2**31), 2**31 - 1, 0], type=pa.int32()),
            pa.array([-(2**63), 2**63 - 1, 0], type=pa.int64()),
            pa.array([-(2**24), 2**24 - 1, None], type=pa.float32()),
            pa.array([-(2**53), 2**53 - 1, None], type=pa.float64()),
            pa.array(
                ["\xef\xff\xdc\xde\xee", "\xfa\xfb\xdd\xaf\xaa", None], type=pa.utf8()
            ),
            pa.array([b"\xde\xad\xbe\xef", b"\xff\xee", None], type=pa.binary()),
            pa.array([234, 42, None], type=pa.time64("us")),
        ],
        names=[
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
            "binary",
            "time64us",
        ],
    )

    return tbl


def basic_dataframe():
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
                pd.to_datetime("2019-01-01"),
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
            "non-ascii": "string",
        }
    )

    # See pandas GH issue #56994
    df["binary"] = pa.array([b"\xde\xad\xbe\xef", b"\xff\xee", None], type=pa.binary())
    df["binary"] = df["binary"].astype("binary[pyarrow]")
    df["time64us"] = pd.DataFrame(
        {"col": pa.array([234, 42, None], type=pa.time64("us"))}
    )
    df["time64us"] = df["time64us"].astype("time64[us][pyarrow]")

    return df


@pytest.fixture(params=[basic_arrow_table, basic_dataframe])
def frame(request):
    """Fixture to use which should contain all data types."""
    return request.param()


def roundtripped_pandas():
    """Roundtripped DataFrames should use arrow dtypes by default"""
    df = basic_dataframe()
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
            "object": "large_string[pyarrow]",
            "int16_limits": "int16[pyarrow]",
            "int32_limits": "int32[pyarrow]",
            "int64_limits": "int64[pyarrow]",
            "float32_limits": "double[pyarrow]",
            "float64_limits": "double[pyarrow]",
            "non-ascii": "large_string[pyarrow]",
            "string": "large_string[pyarrow]",
            "binary": "large_binary[pyarrow]",
            "time64us": "time64[us][pyarrow]",
        }
    )
    return df


@pytest.fixture(params=[("pandas", roundtripped_pandas)])
def roundtripped(request):
    result_obj = request.param[1]()
    return (request.param[0], result_obj)


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
