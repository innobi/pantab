import datetime
import pathlib

import narwhals as nw
import numpy as np
import pandas as pd
import pandas.testing as tm
import polars as pl
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
            ("datetime64_utc", pa.timestamp("us", "UTC")),
            ("object", pa.large_string()),
            ("string", pa.string()),
            ("int16_limits", pa.int16()),
            ("int32_limits", pa.int32()),
            ("int64_limits", pa.int64()),
            ("float32_limits", pa.float32()),
            ("float64_limits", pa.float64()),
            ("oid", pa.uint32()),
            ("non-ascii", pa.utf8()),
            ("json", pa.large_string()),
            ("binary", pa.binary()),
            ("interval", pa.month_day_nano_interval()),
            ("time64us", pa.time64("us")),
            ("geography", pa.large_binary()),
            ("decimal", pa.decimal128(38, 10)),
            ("string_view", pa.string_view()),
            ("binary_view", pa.binary_view()),
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
            pa.array([1, 42, None]),
            pa.array(
                ["\xef\xff\xdc\xde\xee", "\xfa\xfb\xdd\xaf\xaa", None],
            ),
            pa.array(['{"foo": 42}', '{"bar": -42}', None]),
            pa.array([b"\xde\xad\xbe\xef", b"\xff\xee", None]),
            pa.array(
                [
                    pa.scalar((1, 15, -30000), type=pa.month_day_nano_interval()),
                    pa.scalar((-1, -15, 30000), type=pa.month_day_nano_interval()),
                    None,
                ]
            ),
            pa.array([234, 42, None]),
            pa.array(
                [
                    b"\x07\xaa\x02\xe0%n\xd9\x01\x01\n\x00\xce\xab\xe8\xfa=\xff\x96\xf0\x8a\x9f\x01",
                    b"\x07\xaa\x02\x0c&n\x82\x01\x01\n\x00\xb0\xe2\xd4\xcc>\xd4\xbc\x97\x88\x0f",
                    None,
                ]
            ),
            pa.array(["1234567890.123456789", "99876543210.987654321", None]),
            pa.array(["foo", "longer_than_prefix_size", None], type=pa.string_view()),
            pa.array([b"foo", b"longer_than_prefix_size", None], type=pa.binary_view()),
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
                1,
                "\xef\xff\xdc\xde\xee",
                '{"foo": 42}',
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
                42,
                "\xfa\xfb\xdd\xaf\xaa",
                '{"bar": -42}',
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
                pd.NA,
                np.nan,
                pd.NA,
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
            "oid",
            "non-ascii",
            "json",
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
            "float32_limits": np.float32,
            "float64_limits": np.float64,
            "oid": "UInt32",
            "non-ascii": "string",
            "json": "string",
        }
    )

    # See pandas GH issue #56994
    df["binary"] = pa.array([b"\xde\xad\xbe\xef", b"\xff\xee", None], type=pa.binary())
    df["binary"] = df["binary"].astype("binary[pyarrow]")
    # pandas interval support is broken in pyarrow < 16
    # df["interval"] = pa.array(
    #    [
    #        pa.scalar((1, 15, -30000), type=pa.month_day_nano_interval()),
    #        pa.scalar((-1, -15, 30000), type=pa.month_day_nano_interval()),
    #        None,
    #    ]
    # )
    # df["interval"] = df["interval"].astype("month_day_nano_interval[pyarrow]")
    df["time64us"] = pd.DataFrame(
        {"col": pa.array([234, 42, None], type=pa.time64("us"))}
    )
    df["time64us"] = df["time64us"].astype("time64[us][pyarrow]")
    df["geography"] = pa.array(
        [
            b"\x07\xaa\x02\xe0%n\xd9\x01\x01\n\x00\xce\xab\xe8\xfa=\xff\x96\xf0\x8a\x9f\x01",
            b"\x07\xaa\x02\x0c&n\x82\x01\x01\n\x00\xb0\xe2\xd4\xcc>\xd4\xbc\x97\x88\x0f",
            None,
        ]
    )
    df["geography"] = df["geography"].astype("large_binary[pyarrow]")

    df["decimal"] = pd.Series(
        ["1234567890.123456789", "99876543210.987654321", None],
        dtype=pd.ArrowDtype(pa.decimal128(38, 10)),
    )
    """
    df["string_view"] = pd.Series(
        ["foo", "longer_than_prefix_size", None],
        dtype=pd.ArrowDtype(pa.string_view())),
    df["binary_view"] = pd.Series(
        [b"foo", b"longer_than_prefix_size", None],
        dtype=pd.ArrowDtype(pa.binary_view())),
    """

    return df


def basic_polars_frame():
    tbl = basic_arrow_table()

    # polars does not support month_day_nano_interval yet
    tbl = tbl.drop_columns(["interval"])
    df = pl.from_arrow(tbl)
    return df


@pytest.fixture(params=[basic_arrow_table, basic_dataframe, basic_polars_frame])
def frame(request):
    """Fixture to use which should contain all data types."""
    return request.param()


def chunked_pyarrow_table():
    arr = pa.chunked_array([[1, 2, 3], [4, 5, 6]])
    table = pa.table([arr], names=["int"])
    return table


def chunked_polars_frame():
    tbl = chunked_pyarrow_table()
    return pl.from_arrow(tbl)


@pytest.fixture(params=[chunked_pyarrow_table, chunked_polars_frame])
def chunked_frame(request):
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
            ("float32", pa.float32()),
            ("float64", pa.float64()),
            ("Float32", pa.float32()),
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
            ("float32_limits", pa.float32()),
            ("float64_limits", pa.float64()),
            ("oid", pa.uint32()),
            ("non-ascii", pa.large_string()),
            ("json", pa.large_string()),
            ("binary", pa.large_binary()),
            ("interval", pa.month_day_nano_interval()),
            ("time64us", pa.time64("us")),
            ("geography", pa.large_binary()),
            ("decimal", pa.decimal128(38, 10)),
            # ("string_view", pa.large_string()),
            # ("binary_view", pa.large_binary()),
        ]
    )
    tbl = basic_arrow_table()

    # pyarrow does not support casting from string_view to large_string,
    # so we have to handle manually
    tbl = tbl.drop_columns(["string_view", "binary_view"])
    tbl = tbl.cast(schema)

    sv = (pa.array(["foo", "longer_than_prefix_size", None], type=pa.large_string()),)
    bv = pa.array([b"foo", b"longer_than_prefix_size", None], type=pa.large_binary())
    tbl = tbl.append_column("string_view", sv)
    tbl = tbl.append_column("binary_view", bv)

    return tbl


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
            "float32": "float[pyarrow]",
            "float64": "double[pyarrow]",
            "Float32": "float[pyarrow]",
            "Float64": "double[pyarrow]",
            "bool": "boolean[pyarrow]",
            "boolean": "boolean[pyarrow]",
            "datetime64": "timestamp[us][pyarrow]",
            "datetime64_utc": "timestamp[us, UTC][pyarrow]",
            "object": "large_string[pyarrow]",
            "int16_limits": "int16[pyarrow]",
            "int32_limits": "int32[pyarrow]",
            "int64_limits": "int64[pyarrow]",
            "float32_limits": "float[pyarrow]",
            "float64_limits": "double[pyarrow]",
            "oid": "uint32[pyarrow]",
            "non-ascii": "large_string[pyarrow]",
            "json": "large_string[pyarrow]",
            "string": "large_string[pyarrow]",
            "binary": "large_binary[pyarrow]",
            # "interval": "month_day_nano_interval[pyarrow]",
            "time64us": "time64[us][pyarrow]",
            "geography": "large_binary[pyarrow]",
            # "string_view": "string_view[pyarrow]",
            # "binary_view": "binary_view[pyarrow]",
        }
    )
    return df


def roundtripped_polars():
    df = basic_polars_frame()
    return df


@pytest.fixture(
    params=[
        ("pandas", roundtripped_pandas),
        ("pyarrow", roundtripped_pyarrow),
        ("polars", roundtripped_polars),
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
        elif isinstance(result, pl.DataFrame):
            assert result.equals(expected)
        else:
            raise NotImplementedError("assert_frame_equal not implemented for type")

    @staticmethod
    def concat_frames(frame1, frame2):
        assert isinstance(frame1, type(frame2))
        if isinstance(frame1, pd.DataFrame):
            return pd.concat([frame1, frame2]).reset_index(drop=True)
        elif isinstance(frame1, pa.Table):
            return pa.concat_tables([frame1, frame2])
        elif isinstance(frame1, pl.DataFrame):
            return pl.concat([frame1, frame2])
        else:
            raise NotImplementedError("concat_frames not implemented for type")

    @staticmethod
    def empty_like(frame):
        if isinstance(frame, pd.DataFrame):
            return frame.iloc[:0]
        elif isinstance(frame, pa.Table):
            return frame.schema.empty_table()
        elif isinstance(frame, pl.DataFrame):
            return frame.filter(False)
        else:
            raise NotImplementedError("empty_like not implemented for type")

    @staticmethod
    @nw.narwhalify
    def drop_columns(frame, columns):
        return frame.drop(columns)

    @staticmethod
    def select_columns(frame, columns):
        if isinstance(frame, pd.DataFrame):
            return frame[columns]
        elif isinstance(frame, pa.Table):
            return frame.select(columns)
        elif isinstance(frame, pl.DataFrame):
            return frame.select(columns)
        else:
            raise NotImplementedError("select_columns not implemented for type")

    @staticmethod
    @nw.narwhalify
    def cast_column_to_type(frame, column, type_):
        return frame.with_columns(nw.col(column).cast(type_))

    @staticmethod
    def add_non_writeable_column(frame):
        if isinstance(frame, pd.DataFrame):
            frame["should_fail"] = pd.Series([list((1, 2))])
            return frame
        elif isinstance(frame, pa.Table):
            new_column = pa.array([[1, 2], None, None])
            frame = frame.append_column("should_fail", new_column)
            return frame
        elif isinstance(frame, pl.DataFrame):
            frame = frame.with_columns(
                pl.Series(
                    name="should_fail",
                    values=[list((1, 2)), list((1, 2)), list((1, 2))],
                )
            )
            return frame
        else:
            raise NotImplementedError("test not implemented for object")


@pytest.fixture()
def compat():
    return Compat
