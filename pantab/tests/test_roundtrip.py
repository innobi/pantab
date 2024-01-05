import numpy as np
import pandas as pd
import pandas.testing as tm
from tableauhyperapi import TableName

import pantab


def assert_roundtrip_equal(result, expected):
    """Compat helper for comparing round-tripped results."""

    expected["object"] = expected["object"].astype("string")
    expected["non-ascii"] = expected["non-ascii"].astype("string")
    # expected["datetime64_utc"] = expected["datetime64_utc"].dt.tz_localize(None)

    tm.assert_frame_equal(result, expected)


def test_basic(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)
    # expected["Float32"] = expected["Float32"].astype(np.float64)
    # expected["Float64"] = expected["Float64"].astype(np.float64)

    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    assert_roundtrip_equal(result, expected)


def test_multiple_tables(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: df, "table2": df}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = df.copy()
    expected["float32"] = expected["float32"].astype(np.float64)
    # expected["Float32"] = expected["Float32"].astype(np.float64)
    # expected["Float64"] = expected["Float64"].astype(np.float64)
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set((table_name, TableName("public", "table2")))
    for val in result.values():
        assert_roundtrip_equal(val, expected)
