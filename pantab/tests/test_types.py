import pytest
import tableauhyperapi as tab_api

import pantab._types


@pytest.mark.parametrize(
    "nullability", [tab_api.Nullability.NULLABLE, tab_api.Nullability.NOT_NULLABLE]
)
def test_read_varchar_type(nullability):
    """
    Test that we can read a VARCHAR column from Hyper.
    """
    vchar = tab_api.SqlType.varchar(255)
    vchar_column = pantab._types._ColumnType(vchar, nullability)
    assert pantab._types._get_pandas_type(vchar_column) == "string"
