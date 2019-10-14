import os
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

    @pytest.mark.skip("Not possible with Tableau API...")
    def test_frame_to_rows(self):
        """Ideally we would have an individual function / unit test to convert
        a DataFrame to a list of Rows. However, from what I can tell there is
        no way in the Tableau SDK to "get" attributes of a ``Row`` object
        (only setters are exposed) so I am not sure on how to implement this.

        Still wanted to keep track of this however as it is important."""
        pass

    def test_frame_from_file_raises(self, df):
        with pytest.raises(
            NotImplementedError, match="Not possible with " "current SDK"
        ):
            pantab.frame_from_hyper("foo.hyper")


@pytest.mark.skip("Not yet implemented...")
class TestIntegrations:
    @pytest.fixture(autouse=True)
    def setup(self):
        file_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(file_dir, "data")
        self.data_dir = data_dir

    def test_roundtrip(self, df):
        test_data = os.path.join(self.data_dir, "test.hyper")
        with open(test_data, "rb") as infile:
            infile.read()

        # Ideally we could just use a buffer, but the Tableau SDK
        # requires a physical string to be passed to the Extract object
        # Because it creates more than just the .hyper file, we need to
        # create a temporary directory for it to write to
        with tempfile.TemporaryDirectory() as tmp:
            fn = os.path.join(tmp, "test.hyper")
            pantab.frame_to_hyper(df, fn)
            comp = pantab.frame_from_hyper(fn)

        # Because Tableau only supports the 64 bit variants, upcast the
        # particular df dtypes that are lower bit
        df = df.astype({"foo": np.int64, "bar": np.int64, "qux": np.float64})

        tm.assert_frame_equal(df, comp)
