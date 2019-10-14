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


@pytest.fixture
def datapath():
    """Path to directory containing test data"""
    yield pathlib.Path(__file__) / "../data"


class TestPanTab:
    @pytest.mark.parametrize(
        "typ_in,typ_out",
        [
            ("int16", TypeTag.SMALL_INT),
            ("int32", TypeTag.INT),
            ("int64", TypeTag.BIG_INT),
            ("float32", TypeTag.DOUBLE),
            ("float64", TypeTag.DOUBLE),
            ("bool", TypeTag.BOOL),
            ("datetime64[ns]", TypeTag.TIMESTAMP),
            ("object", TypeTag.TEXT),
        ],
    )
    def test_pan_to_tab_types(self, typ_in, typ_out):
        assert pantab.pandas_to_tableau_type(typ_in) == typ_out

    @pytest.mark.parametrize(
        "typ_in",
        ["timedelta64[ns, tz]", "categorical", "complex128", "timedelta64[ns]"],
    )
    def test_pan_to_tab_types_raises(self, typ_in):
        with pytest.raises(
            TypeError,
            match="Conversion of '{}' dtypes "
            "not yet supported!".format(re.escape(typ_in)),
        ):
            pantab.pandas_to_tableau_type(typ_in)

    @pytest.mark.parametrize(
        "typ_in,typ_out",
        [
            (TypeTag.BIG_INT, "int64"),
            (TypeTag.DOUBLE, "float64"),
            (TypeTag.BOOL, "bool"),
            (TypeTag.TIMESTAMP, "datetime64[ns]"),
            (TypeTag.TEXT, "object"),
        ],
    )
    def test_tab_to_pan_types(self, typ_in, typ_out):
        assert pantab.tableau_to_pandas_type(typ_in) == typ_out

    def test_types_for_columns(self, df):
        exp = (
            TypeTag.SMALL_INT,
            TypeTag.INT,
            TypeTag.BIG_INT,
            TypeTag.DOUBLE,
            TypeTag.DOUBLE,
            TypeTag.BOOL,
            TypeTag.TIMESTAMP,
            TypeTag.TEXT,
        )

        assert pantab._types_for_columns(df) == exp

    def test_test_roundtrip(self, datapath):
        fn = (datapath / "test.hyper").resolve()
        table_name = "some_table"

        df =  pd.DataFrame(
            [[1, 2, 3, 4., 5., True, pd.to_datetime('1/1/18'), 'foo'],
             [6, 7, 8, 9., 10., True, pd.to_datetime('1/1/19'), 'foo']
             ], columns=['foo', 'bar', 'baz', 'qux', 'quux', 'quuuz', 'corge',
                         'garply'])

        pantab.frame_to_hyper(df, fn, table_name)
        result = pantab.frame_from_hyper(fn, table_name)
        expected = df

        tm.assert_frame_equal(result, expected)

