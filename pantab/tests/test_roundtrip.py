import pyarrow as pa
from tableauhyperapi import TableName

import pantab


def test_basic(frame, roundtripped, tmp_hyper, table_name, table_mode, compat):
    return_type, expected = roundtripped
    if not (isinstance(frame, pa.Table) and return_type == "pyarrow"):
        frame = compat.drop_columns(frame, ["interval"])
        expected = compat.drop_columns(expected, ["interval"])

    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(frame, tmp_hyper, table=table_name, table_mode=table_mode)

    result = pantab.frame_from_hyper(
        tmp_hyper, table=table_name, return_type=return_type
    )

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    compat.assert_frame_equal(result, expected)


def test_multiple_tables(
    frame, roundtripped, tmp_hyper, table_name, table_mode, compat
):
    return_type, expected = roundtripped
    if not (isinstance(frame, pa.Table) and return_type == "pyarrow"):
        frame = compat.drop_columns(frame, ["interval"])
        expected = compat.drop_columns(expected, ["interval"])

    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frames_to_hyper(
        {table_name: frame, "table2": frame}, tmp_hyper, table_mode=table_mode
    )
    pantab.frames_to_hyper(
        {table_name: frame, "table2": frame}, tmp_hyper, table_mode=table_mode
    )

    result = pantab.frames_from_hyper(tmp_hyper, return_type=return_type)

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set((table_name, TableName("public", "table2")))
    for val in result.values():
        compat.assert_frame_equal(val, expected)


def test_empty_roundtrip(
    frame, roundtripped, tmp_hyper, table_name, table_mode, compat
):
    return_type, expected = roundtripped
    if not (isinstance(frame, pa.Table) and return_type == "pyarrow"):
        frame = compat.drop_columns(frame, ["interval"])
        expected = compat.drop_columns(expected, ["interval"])

    # object case is by definition vague, so lets punt that for now
    frame = compat.drop_columns(frame, ["object"])
    empty = compat.empty_like(frame)
    pantab.frame_to_hyper(empty, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(empty, tmp_hyper, table=table_name, table_mode=table_mode)

    result = pantab.frame_from_hyper(
        tmp_hyper, table=table_name, return_type=return_type
    )

    expected = compat.drop_columns(expected, ["object"])
    expected = compat.empty_like(expected)
    compat.assert_frame_equal(result, expected)
