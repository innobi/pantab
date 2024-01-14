import pandas as pd
import pandas.testing as tm
from tableauhyperapi import TableName

import pantab


def test_basic(df, roundtripped, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = roundtripped
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

        # TODO: somehow concat turns string[pyarrow] into string python
        for col in ("object", "non-ascii", "string"):
            expected[col] = expected[col].astype("string[pyarrow]")

    tm.assert_frame_equal(result, expected)


def test_multiple_tables(df, roundtripped, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = roundtripped
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

        # TODO: somehow concat turns string[pyarrow] into string python
        for col in ("object", "non-ascii", "string"):
            expected[col] = expected[col].astype("string[pyarrow]")

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set((table_name, TableName("public", "table2")))
    for val in result.values():
        tm.assert_frame_equal(val, expected)
