from tableausdk.Types import Type as ttypes
import numpy as np
import pytest

import pantab


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
