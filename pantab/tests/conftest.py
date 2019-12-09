import numpy as np
from time import time
import pandas as pd
import pytest
import tableauhyperapi as tab_api


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
        ],
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
            "datetime64_utc": "datetime64[ns, UTC]",
            "timedelta64": "timedelta64[ns]",
            "object": "object",
        }
    )

    return df


@pytest.fixture(params=[1, 25, 100])
def rnd_df(request):
    """Generates randomized filled DataFrame."""

    def random_date():
        return pd.to_datetime(np.random.randint(0, int(time())), unit="s")

    rnd_df = pd.DataFrame(
        {
            "keys": ["ID" + str(_ + 1).zfill(5) for _ in range(request.param)],
            "ints": np.random.randint(0, 256 + 1, request.param),
            "floats": np.random.randn(request.param),
            "booleans": [np.random.choice([True, False]) for _ in range(request.param)],
            "dates": [random_date() for _ in range(request.param)],
            "partials": [
                np.random.choice([np.nan, "x", "y"]) for _ in range(request.param)
            ],
        }
    )

    return rnd_df


@pytest.fixture
def tmp_hyper(tmp_path):
    """A temporary file name to write / read a Hyper extract from."""
    return tmp_path / "test.hyper"


@pytest.fixture(params=["w", "a"])
def table_mode(request):
    """Write or append markers for table handling."""
    return request.param


@pytest.fixture(params=["i"])
def update_mode(request):
    """Separate test for insert table mode as it requires an existing table."""
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
