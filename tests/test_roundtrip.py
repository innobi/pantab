import datetime
import decimal

import pandas as pd
import pyarrow as pa
import pytest

try:
    import tableauhyperapi as tab_api
except ModuleNotFoundError:
    has_tableauhyperapi = False
else:
    del tab_api
    has_tableauhyperapi = True

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
        process_params={"default_database_version": "4"},
    )
    pt.frame_to_hyper(
        frame,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
        process_params={"default_database_version": "4"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table=table_name, return_type=return_type)

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    if isinstance(frame, pd.DataFrame) and return_type != "pandas":
        expected = compat.drop_columns(expected, ["string_view", "binary_view"])

    if return_type == "pandas" and not isinstance(frame, pd.DataFrame):
        result = compat.drop_columns(result, ["string_view", "binary_view"])

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
        process_params={"default_database_version": "4"},
    )
    pt.frames_to_hyper(
        {table_name: frame, "table2": frame},
        tmp_hyper,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
        process_params={"default_database_version": "4"},
    )

    result = pt.frames_from_hyper(tmp_hyper, return_type=return_type)

    if table_mode == "a":
        expected = compat.concat_frames(expected, expected)

    if hasattr(table_name, "_unescaped_components"):  # tableauhyperapi TableName
        if table_name.schema_name:
            exp_schema = table_name.schema_name.name.unescaped
        else:
            exp_schema = "public"

        exp_table = table_name.name.unescaped
    elif hasattr(table_name, "unescaped"):  # tableauhyperapi Name
        exp_schema = "public"
        exp_table = table_name.unescaped
    else:
        exp_schema = "public"
        exp_table = table_name

    if isinstance(frame, pd.DataFrame) and return_type != "pandas":
        expected = compat.drop_columns(expected, ["string_view", "binary_view"])

    assert set(result.keys()) == set(
        (
            (exp_schema, exp_table),
            ("public", "table2"),
        )
    )
    for val in result.values():
        if return_type == "pandas" and not isinstance(frame, pd.DataFrame):
            val = compat.drop_columns(val, ["string_view", "binary_view"])

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
        process_params={"default_database_version": "4"},
    )
    pt.frame_to_hyper(
        empty,
        tmp_hyper,
        table=table_name,
        table_mode=table_mode,
        json_columns={"json"},
        geo_columns={"geography"},
        process_params={"default_database_version": "4"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table=table_name, return_type=return_type)

    if isinstance(frame, pd.DataFrame) and return_type != "pandas":
        expected = compat.drop_columns(expected, ["string_view", "binary_view"])

    if return_type == "pandas" and not isinstance(frame, pd.DataFrame):
        result = compat.drop_columns(result, ["string_view", "binary_view"])

    expected = compat.drop_columns(expected, ["object"])
    expected = compat.empty_like(expected)
    compat.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "needs_hyperapi, hyperapi_obj, table_args",
    [
        (False, None, tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",))),
        (True, "Name", tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",))),
        (
            True,
            "TableName",
            tuple(("a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",)),
        ),
        (
            True,
            "TableName",
            (
                "a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",
                "a';DROP TABLE users;DELETE FROM foo WHERE 't' = 't",
            ),
        ),
    ],
)
def test_write_prevents_injection(tmp_hyper, needs_hyperapi, hyperapi_obj, table_args):
    if not needs_hyperapi:
        table = table_args[0]
    else:
        tab_api = pytest.importorskip("tableauhyperapi")
        table = getattr(tab_api, hyperapi_obj)(*table_args)

    frame = pd.DataFrame(list(range(10)), columns=["nums"]).astype("int8")
    frames = {table: frame}
    pt.frames_to_hyper(frames, tmp_hyper)
    pt.frames_from_hyper(tmp_hyper)


@pytest.mark.parametrize(
    "value,precision,scale",
    [
        ("0.00", 3, 2),
        ("0E-10", 3, 2),
        ("100", 3, 0),
        ("1.00", 3, 2),
        (".001", 3, 3),
    ],
)
def test_decimal_roundtrip(tmp_hyper, value, precision, scale, compat):
    arr = pa.array([decimal.Decimal(value)], type=pa.decimal128(precision, scale))
    tbl = pa.Table.from_arrays([arr], names=["col"])
    pt.frame_to_hyper(tbl, tmp_hyper, table="test")
    result = pt.frame_from_hyper(tmp_hyper, table="test", return_type="pyarrow")
    compat.assert_frame_equal(result, tbl)


def test_date32_time_t_limits_roundtrip(tmp_hyper, compat):
    arr = pa.array(
        [
            datetime.date(2262, 4, 11),
            datetime.date(2262, 4, 12),
            datetime.date(2262, 4, 13),
        ],
        type=pa.date32(),
    )
    tbl = pa.Table.from_arrays([arr], names=["col"])
    pt.frame_to_hyper(tbl, tmp_hyper, table="test")
    result = pt.frame_from_hyper(tmp_hyper, table="test", return_type="pyarrow")
    compat.assert_frame_equal(result, tbl)


def test_chunked_data_roundtrip(frame, tmp_hyper, compat):
    if not isinstance(frame, pa.Table):
        pytest.skip("only testing for pyarrow roundtrip")

    pt.frame_to_hyper(
        frame.to_reader(1),
        tmp_hyper,
        table="test",
        process_params={"default_database_version": "4"},
    )
    result = pt.frame_from_hyper(tmp_hyper, table="test", return_type="pyarrow")
    expected = frame.cast(result.schema)

    compat.assert_frame_equal(result, expected)
