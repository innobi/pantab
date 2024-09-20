import pyarrow as pa
from tableauhyperapi import TableName

import pantab as pt


def test_basic(frame, roundtripped, tmp_hyper, table_name, table_mode, compat):
    return_type, expected = roundtripped
    if isinstance(frame, pa.Table) and return_type != "pyarrow":
        frame = compat.drop_columns(frame, ["interval"])

    if return_type == "pyarrow" and not isinstance(frame, pa.Table):
        expected = compat.drop_columns(expected, ["interval"])

    # Write twice; depending on mode this should either overwrite or duplicate entries
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table=table_name, return_type=return_type)

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    compat.assert_frame_equal(result, expected)


def test_multiple_tables(
    frame, roundtripped, tmp_hyper, table_name, table_mode, compat
):
    return_type, expected = roundtripped
    if isinstance(frame, pa.Table) and return_type != "pyarrow":
        frame = compat.drop_columns(frame, ["interval"])

    if return_type == "pyarrow" and not isinstance(frame, pa.Table):
        expected = compat.drop_columns(expected, ["interval"])

    # Write twice; depending on mode this should either overwrite or duplicate entries
    pt.frames_to_hyper(
        {table_name: frame, "table2": frame},
        tmp_hyper,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )
    pt.frames_to_hyper(
        {table_name: frame, "table2": frame},
        tmp_hyper,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )

    result = pt.frames_from_hyper(tmp_hyper, return_type=return_type)

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    # some test trickery here
    if not isinstance(table_name, TableName) or table_name.schema_name is None:
        table_name = TableName("public", table_name)

    assert set(result.keys()) == set(
        (
            ".".join(table_name._unescaped_components),
            ".".join(TableName("public", "table2")._unescaped_components),
        )
    )
    for val in result.values():
        compat.assert_frame_equal(val, expected)


def test_empty_roundtrip(
    frame, roundtripped, tmp_hyper, table_name, table_mode, compat
):
    return_type, expected = roundtripped
    if isinstance(frame, pa.Table) and return_type != "pyarrow":
        frame = compat.drop_columns(frame, ["interval"])

    if return_type == "pyarrow" and not isinstance(frame, pa.Table):
        expected = compat.drop_columns(expected, ["interval"])

    # object case is by definition vague, so lets punt that for now
    frame = compat.drop_columns(frame, ["object"])
    empty = compat.empty_like(frame)
    pt.frame_to_hyper(
        empty,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )
    pt.frame_to_hyper(
        empty,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table=table_name, return_type=return_type)

    expected = compat.drop_columns(expected, ["object"])
    expected = compat.empty_like(expected)
    compat.assert_frame_equal(result, expected)
