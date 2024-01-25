import pandas as pd
import pandas.testing as tm
import pyarrow as pa
from tableauhyperapi import TableName

import pantab


def test_basic(frame, roundtripped, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = roundtripped
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    tm.assert_frame_equal(result, expected)


def test_multiple_tables(frame, roundtripped, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: frame, "table2": frame}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: frame, "table2": frame}, tmp_hyper, table_mode=table_mode
    )
    result = pantab.frames_from_hyper(tmp_hyper)

    expected = roundtripped
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set((table_name, TableName("public", "table2")))
    for val in result.values():
        tm.assert_frame_equal(val, expected)


def test_empty_roundtrip(frame, roundtripped, tmp_hyper, table_name, table_mode):
    # object case is by definition vague, so lets punt that for now
    frame = frame.drop(columns=["object"])

    if isinstance(frame, pa.Table):
        empty = frame.schema.empty_table()
    else:
        empty = frame.iloc[:0]
    pantab.frame_to_hyper(empty, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(empty, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = roundtripped.iloc[:0]
    expected = expected.drop(columns=["object"])
    tm.assert_frame_equal(result, expected)
