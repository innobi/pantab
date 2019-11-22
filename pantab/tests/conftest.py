import numpy as np
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
