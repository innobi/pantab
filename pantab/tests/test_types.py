import tableauhyperapi as tab_api

import pantab._types


def test_read_varchar_type():
    """
    Test that we can read a VARCHAR column from Hyper.
    """
    vchar = tab_api.SqlType.varchar(255)
    vchar_column = pantab._types._ColumnType(vchar, tab_api.Nullability.NULLABLE)
    assert pantab._types._get_pandas_type(vchar_column) == "string"


def test_read_varchar_type_non_null():
    """
    Test that we can read a VARCHAR column from Hyper that is non nullable.
    """
    vchar = tab_api.SqlType.varchar(255)
    vchar_column = pantab._types._ColumnType(vchar, tab_api.Nullability.NOT_NULLABLE)
    assert pantab._types._get_pandas_type(vchar_column) == "string"
