from tableausdk.Types import Type as ttypes
import numpy as np
import pandas as pd
import pytest

import pantab

@pytest.fixture
def df():
    df = pd.DataFrame(
        [[1, 2, 3, 4., 5., True, pd.to_datetime('1/1/18'), pd.Timedelta(days=1),
          'foo'],
         [6, 7, 8, 9., 10., True, pd.to_datetime('1/1/19'), pd.Timedelta(
             days=-1), 'foo']
        ], columns=['foo', 'bar', 'baz', 'qux', 'quux', 'quuuz', 'corge',
                    'grault', 'garply'])

    df = df.astype({
        'foo' : np.int16,
        'bar': np.int32,
        'baz': np.int64,
        'qux': np.float32,
        'quux': np.float64,
        'quuuz': np.bool,
        'corge': 'datetime64[ns]',
        'grault': 'timedelta64[ns]',
        'garply': 'object'
    })

    return df


class TestPanTab():

    @pytest.mark.parametrize("typ_in,typ_out", [
        ('int16', ttypes.INTEGER), ('int32', ttypes.INTEGER),
        ('int64', ttypes.INTEGER), ('float32', ttypes.DOUBLE),
        ('float64', ttypes.DOUBLE), ('bool', ttypes.BOOLEAN),
        ('datetime64[ns]', ttypes.DATETIME),
        ('timedelta64[ns]', ttypes.DURATION), ('object', ttypes.UNICODE_STRING)
    ])
    def test_pan_to_tab_types(self, typ_in, typ_out):
        assert pantab.pandas_to_tableau_type(typ_in) == typ_out

    @pytest.mark.parametrize("typ_in", [
        'timedelta64[ns, tz]', 'categorical', 'complex128'
    ])
    def test_pan_to_tab_types_raises(self, typ_in):
        with pytest.raises(TypeError, message="Conversion of '{}' dtypes "
                                    "not yet supported!".format(typ_in)):
            pantab.pandas_to_tableau_type(typ_in)

    @pytest.mark.parametrize("typ_in,typ_out", [
        (ttypes.INTEGER, 'int64'), (ttypes.DOUBLE, 'float64'),
        (ttypes.BOOLEAN, 'bool'), (ttypes.DATETIME, 'datetime64[ns]'),
        (ttypes.DURATION, 'timedelta64[ns]'), (ttypes.UNICODE_STRING, 'object')
    ])
    def test_pan_to_tab_types(self, typ_in, typ_out):
        assert pantab.tableau_to_pandas_type(typ_in) == typ_out        

    def test_types_for_columns(self, df):
        exp = (ttypes.INTEGER, ttypes.INTEGER, ttypes.INTEGER, ttypes.DOUBLE,
               ttypes.DOUBLE, ttypes.BOOLEAN, ttypes.DATETIME, ttypes.DURATION,
               ttypes.UNICODE_STRING)

        assert pantab._types_for_columns(df) == exp

    @pytest.mark.parametrize("typ,exp", [
        (ttypes.INTEGER, 'setInteger'),
        (ttypes.DOUBLE, 'setDouble'),
        (ttypes.BOOLEAN, 'setBoolean'),
        (ttypes.DATETIME, 'setDateTime'),
        (ttypes.DURATION, 'setDuration'),
        (ttypes.UNICODE_STRING, 'setString')
    ])
    def test_accessors_for_tableau_type(self, typ, exp):
        assert pantab._accessor_for_tableau_type(typ) == exp

    @pytest.mark.parametrize("val,accsr,exp", [
        (pd.to_datetime('1/2/18 01:23:45.6789'), 'setDateTime',
         [2018, 1, 2, 1, 23, 45, 6789]),
        (pd.Timedelta('1 days 01:23:45.6789'), 'setDuration',
         [1, 1, 23, 45, 6789]),
        (1, 'setInteger', [1]), ('foo', 'setString', ['foo'])
    ])
    def test_append_args_for_val_and_accessor(self, val, accsr, exp):
        args = list()
        pantab._append_args_for_val_and_accessor(args, val, accsr)
        assert args == exp            

    @pytest.mark.skip("Not possible with Tableau API...")
    def test_frame_to_rows(self):
        """Ideally we would have an individual function / unit test to convert
        a DataFrame to a list of Rows. However, from what I can tell there is
        no way in the Tableau SDK to "get" attributes of a ``Row`` object
        (only setters are exposed) so I am not sure on how to implement this.

        Still wanted to keep track of this however as it is important."""
        pass


class TestIntegrations():

    pass
