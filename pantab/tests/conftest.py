import datetime
import pathlib

import numpy as np
import pandas as pd
import pandas.testing as tm
import pyarrow as pa
import pytest
import tableauhyperapi as tab_api


def basic_arrow_table():
    schema = pa.schema(
        [
            ("int16", pa.int16()),
            ("int32", pa.int32()),
            ("int64", pa.int64()),
            ("Int16", pa.int16()),
            ("Int32", pa.int32()),
            ("Int64", pa.int64()),
            ("float32", pa.float32()),
            ("float64", pa.float64()),
            ("Float32", pa.float32()),
            ("Float64", pa.float64()),
            ("bool", pa.bool_()),
            ("boolean", pa.bool_()),
            ("date32", pa.date32()),
            ("datetime64", pa.timestamp("us")),
            ("datetime64_utc", pa.timestamp("us", "utc")),
            ("object", pa.large_string()),
            ("string", pa.string()),
            ("int16_limits", pa.int16()),
            ("int32_limits", pa.int32()),
            ("int64_limits", pa.int64()),
            ("float32_limits", pa.float32()),
            ("float64_limits", pa.float64()),
            ("non-ascii", pa.utf8()),
            ("binary", pa.binary()),
            ("time64us", pa.time64("us")),
        ]
    )
    tbl = pa.Table.from_arrays(
        [
            pa.array([1, 6, 0]),
            pa.array([2, 7, 0]),
            pa.array([3, 8, 0]),
            pa.array([1, None, None]),
            pa.array([2, None, None]),
            pa.array([3, None, None]),
            pa.array([4, 9.0, None]),
            pa.array([5, 10.0, None]),
            pa.array([1.0, 1.0, None]),
            pa.array([2.0, 2.0, None]),
            pa.array([True, False, False]),
            pa.array([True, False, None]),
            pa.array(
                [datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), None],
            ),
            pa.array(
                [
                    datetime.datetime(2018, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    None,
                ],
            ),
            pa.array(
                [
                    datetime.datetime(2018, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    None,
                ],
            ),
            pa.array(["foo", "bar", None]),
            pa.array(["foo", "bar", None]),
            pa.array([-(2**15), 2**15 - 1, 0]),
            pa.array([-(2**31), 2**31 - 1, 0]),
            pa.array([-(2**63), 2**63 - 1, 0]),
            pa.array([-(2**24), 2**24 - 1, None]),
            pa.array([-(2**53), 2**53 - 1, None]),
            pa.array(
                ["\xef\xff\xdc\xde\xee", "\xfa\xfb\xdd\xaf\xaa", None],
            ),
            pa.array([b"\xde\xad\xbe\xef", b"\xff\xee", None]),
            pa.array([234, 42, None]),
        ],
        schema=schema,
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


def roundtripped_pyarrow():
    schema = pa.schema(
        [
            ("int16", pa.int16()),
            ("int32", pa.int32()),
            ("int64", pa.int64()),
            ("Int16", pa.int16()),
            ("Int32", pa.int32()),
            ("Int64", pa.int64()),
            ("float32", pa.float64()),
            ("float64", pa.float64()),
            ("Float32", pa.float64()),
            ("Float64", pa.float64()),
            ("bool", pa.bool_()),
            ("boolean", pa.bool_()),
            ("date32", pa.date32()),
            ("datetime64", pa.timestamp("us")),
            ("datetime64_utc", pa.timestamp("us", "UTC")),
            ("object", pa.large_string()),
            ("string", pa.large_string()),
            ("int16_limits", pa.int16()),
            ("int32_limits", pa.int32()),
            ("int64_limits", pa.int64()),
            ("float32_limits", pa.float64()),
            ("float64_limits", pa.float64()),
            ("non-ascii", pa.large_string()),
            ("binary", pa.large_binary()),
            ("time64us", pa.time64("us")),
        ]
    )
    tbl = basic_arrow_table()

    return tbl.cast(schema)


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


@pytest.fixture(
    params=[
        ("pandas", roundtripped_pandas),
        ("pyarrow", roundtripped_pyarrow),
    ]
)
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


class Compat:
    @staticmethod
    def assert_frame_equal(result, expected):
        assert isinstance(result, type(expected))
        if isinstance(result, pd.DataFrame):
            tm.assert_frame_equal(result, expected)
            return
        elif isinstance(result, pa.Table):
            assert result.equals(expected, check_metadata=True)
            return
        else:
            raise NotImplementedError("assert_frame_equal not implemented for type")

    @staticmethod
    def concat_frames(frame1, frame2):
        assert isinstance(frame1, type(frame2))
        if isinstance(frame1, pd.DataFrame):
            return pd.concat([frame1, frame2]).reset_index(drop=True)
        elif isinstance(frame1, pa.Table):
            return pa.concat_tables([frame1, frame2])
        else:
            raise NotImplementedError("concat_frames not implemented for type")

    @staticmethod
    def empty_like(frame):
        if isinstance(frame, pd.DataFrame):
            return frame.iloc[:0]
        elif isinstance(frame, pa.Table):
            return frame.schema.empty_table()
        else:
            raise NotImplementedError("empty_like not implemented for type")

    @staticmethod
    def drop_columns(frame, columns):
        if isinstance(frame, pd.DataFrame):
            return frame.drop(columns=columns)
        elif isinstance(frame, pa.Table):
            return frame.drop_columns(columns)
        else:
            raise NotImplementedError("drop_columns not implemented for type")

    @staticmethod
    def select_columns(frame, columns):
        if isinstance(frame, pd.DataFrame):
            return frame[columns]
        elif isinstance(frame, pa.Table):
            return frame.select(columns)
        else:
            raise NotImplementedError("select_columns not implemented for type")

    @staticmethod
    def cast_column_to_type(frame, column, type_):
        if isinstance(frame, pd.DataFrame):
            frame[column] = frame[column].astype(type_)
            return frame
        elif isinstance(frame, pa.Table):
            schema = pa.schema([pa.field(column, type_)])
            return frame.cast(schema)
        else:
            raise NotImplementedError("cast_column_to_type not implemented for type")

    @staticmethod
    def add_non_writeable_column(frame):
        if isinstance(frame, pd.DataFrame):
            frame["should_fail"] = pd.Series([list((1, 2))])
            return frame
        elif isinstance(frame, pa.Table):
            new_column = pa.array([[1, 2], None, None])
            frame = frame.append_column("should_fail", new_column)
            return frame
        else:
            raise NotImplementedError("test not implemented for object")


@pytest.fixture()
def compat():
    return Compat
