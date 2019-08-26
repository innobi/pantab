import os
import tempfile

from tableausdk.Types import Type as ttypes
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import pantab

@pytest.fixture
def df():
    df = pd.DataFrame(
        [[1, 2, 3, 4., 5., True, pd.to_datetime('1/1/18'), 'foo'],
         [6, 7, 8, 9., 10., True, pd.to_datetime('1/1/19'), 'foo']
        ], columns=['foo', 'bar', 'baz', 'qux', 'quux', 'quuuz', 'corge',
                    'garply'])

    df = df.astype({
        'foo' : np.int16,
        'bar': np.int32,
        'baz': np.int64,
        'qux': np.float32,
        'quux': np.float64,
        'quuuz': np.bool,
        'corge': 'datetime64[ns]',
        #'grault': 'timedelta64[ns]',
        'garply': 'object'
    })

    return df


class TestPanTab():

    @pytest.mark.parametrize("typ_in,typ_out", [
        ('int16', ttypes.INTEGER), ('int32', ttypes.INTEGER),
        ('int64', ttypes.INTEGER), ('float32', ttypes.DOUBLE),
        ('float64', ttypes.DOUBLE), ('bool', ttypes.BOOLEAN),
        ('datetime64[ns]', ttypes.DATETIME), ('object', ttypes.UNICODE_STRING)
    ])
    def test_pan_to_tab_types(self, typ_in, typ_out):
        assert pantab.pandas_to_tableau_type(typ_in) == typ_out

    @pytest.mark.parametrize("typ_in", [
        'timedelta64[ns, tz]', 'categorical', 'complex128', 'timedelta64[ns]'

    ])
    def test_pan_to_tab_types_raises(self, typ_in):
        with pytest.raises(TypeError, message="Conversion of '{}' dtypes "
                                    "not yet supported!".format(typ_in)):
            pantab.pandas_to_tableau_type(typ_in)

    @pytest.mark.parametrize("typ_in,typ_out", [
        (ttypes.INTEGER, 'int64'), (ttypes.DOUBLE, 'float64'),
        (ttypes.BOOLEAN, 'bool'), (ttypes.DATETIME, 'datetime64[ns]'),
        (ttypes.UNICODE_STRING, 'object')
    ])
    def test_tab_to_pan_types(self, typ_in, typ_out):
        assert pantab.tableau_to_pandas_type(typ_in) == typ_out

    def test_types_for_columns(self, df):
        exp = (ttypes.INTEGER, ttypes.INTEGER, ttypes.INTEGER, ttypes.DOUBLE,
               ttypes.DOUBLE, ttypes.BOOLEAN, ttypes.DATETIME,
               ttypes.UNICODE_STRING)

        assert pantab._types_for_columns(df) == exp

    @pytest.mark.parametrize("typ,exp", [
        (ttypes.INTEGER, 'setInteger'),
        (ttypes.DOUBLE, 'setDouble'),
        (ttypes.BOOLEAN, 'setBoolean'),
        (ttypes.DATETIME, 'setDateTime'),
        (ttypes.UNICODE_STRING, 'setString')
    ])
    def test_accessors_for_tableau_type(self, typ, exp):
        assert pantab._accessor_for_tableau_type(typ) == exp

    @pytest.mark.parametrize("val,accsr,exp", [
        (pd.to_datetime('1/2/18 01:23:45.6789'), 'setDateTime',
         [2018, 1, 2, 1, 23, 45, 6789]),
        (1, 'setInteger', [1]), ('foo', 'setString', ['foo'])
    ])
    def test_append_args_for_val_and_accessor(self, val, accsr, exp):
        arg_l = list()
        pantab._append_args_for_val_and_accessor(arg_l, val, accsr)
        assert arg_l == exp

    @pytest.mark.skip("Not possible with Tableau API...")
    def test_frame_to_rows(self):
        """Ideally we would have an individual function / unit test to convert
        a DataFrame to a list of Rows. However, from what I can tell there is
        no way in the Tableau SDK to "get" attributes of a ``Row`` object
        (only setters are exposed) so I am not sure on how to implement this.

        Still wanted to keep track of this however as it is important."""
        pass

    def test_frame_to_file_raises_extract(self, df):
        with pytest.raises(ValueError, message="The Tableau SDK currently only"
                           " supports a table name of 'Extract'"):
            pantab.frame_to_hyper(df, 'foo.hyper', table='foo')

    def test_frame_from_file_raises_extract(self, df):
        with pytest.raises(ValueError, message="The Tableau SDK currently only"
                           " supports a table name of 'Extract'"):
            pantab.frame_from_hyper('foo.hyper', table='foo')

    def test_frame_from_file_raises(self, df):
        with pytest.raises(NotImplementedError, message="Not possible with "
                           "current SDK"):
            pantab.frame_from_hyper('foo.hyper')


@pytest.mark.skip("Not yet implemented...")
class TestIntegrations():

    @pytest.fixture(autouse=True)
    def setup(self):
        file_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(file_dir, 'data')
        self.data_dir = data_dir

    def test_roundtrip(self, df):
        test_data = os.path.join(self.data_dir, 'test.hyper')
        with open(test_data, 'rb') as infile:
            data = infile.read()

        # Ideally we could just use a buffer, but the Tableau SDK
        # requires a physical string to be passed to the Extract object
        # Because it creates more than just the .hyper file, we need to
        # create a temporary directory for it to write to
        with tempfile.TemporaryDirectory() as tmp:
            fn = os.path.join(tmp, 'test.hyper')
            pantab.frame_to_hyper(df, fn)
            comp = pantab.frame_from_hyper(fn)

        # Because Tableau only supports the 64 bit variants, upcast the
        # particular df dtypes that are lower bit
        df = df.astype({
            'foo' : np.int64,
            'bar': np.int64,
            'qux': np.float64,
        })

        tm.assert_frame_equal(df, comp)

