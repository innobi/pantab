import pandas as pd
import pandas.testing as tm
from tableauhyperapi import TableName

import pantab


def convert_df_to_pyarrow(df):
    return df.astype(
        {
            "int16": "int16[pyarrow]",
            "int32": "int32[pyarrow]",
            "int64": "int64[pyarrow]",
            # "Int16": "int16[pyarrow]",
            # "Int32": "int32[pyarrow]",
            # "Int64": "int64[pyarrow]",
            "float32": "double[pyarrow]",
            "float64": "double[pyarrow]",
            # "bool":  "boolean[pyarrow]",
            "datetime64": "timestamp[us][pyarrow]",
            "datetime64_utc": "timestamp[us, UTC][pyarrow]",
            # "timedelta64": "timedelta64[ns]",
            "object": "string[pyarrow]",
            "int16_limits": "int16[pyarrow]",
            "int32_limits": "int32[pyarrow]",
            "int64_limits": "int64[pyarrow]",
            "float32_limits": "double[pyarrow]",
            "float64_limits": "double[pyarrow]",
            "non-ascii": "string[pyarrow]",
            "string": "string[pyarrow]",
        }
    )


def test_basic(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = convert_df_to_pyarrow(df)
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

        # TODO: somehow concat turns string[pyarrow] into string python
        for col in ("object", "non-ascii", "string"):
            expected[col] = expected[col].astype("string[pyarrow]")

    tm.assert_frame_equal(result, expected)


def test_multiple_tables(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = convert_df_to_pyarrow(df)
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
