import pandas as pd
import pandas.testing as tm
import pytest
from tableauhyperapi import TableName

import pantab


def test_read_doesnt_modify_existing_file(df, tmp_hyper):
    pantab.frame_to_hyper(df, tmp_hyper, table="test")
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pantab.frame_from_hyper(tmp_hyper, table="test")
    pantab.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


def test_reports_unsupported_type(datapath):
    """
    Test that we report an error if we encounter an unsupported column type.
    Previously, we did not do so but instead assumed that all unsupported columns
    would be string columns. This led to very fascinating failures.
    """
    db_path = datapath / "geography.hyper"
    with pytest.raises(TypeError, match=r"GEOGRAPHY"):
        pantab.frame_from_hyper(db_path, table="test")


def test_read_non_roundtrippable(datapath):
    result = pantab.frame_from_hyper(
        datapath / "dates.hyper", table=TableName("Extract", "Extract")
    )
    expected = pd.DataFrame(
        [["1900-01-01", "2000-01-01"], [pd.NaT, "2050-01-01"]],
        columns=["Date1", "Date2"],
        dtype="datetime64[ns]",
    )
    tm.assert_frame_equal(result, expected)


def test_reads_non_writeable(datapath):
    result = pantab.frame_from_hyper(
        datapath / "non_pantab_writeable.hyper", table=TableName("public", "table")
    )

    expected = pd.DataFrame(
        [["row1", 1.0], ["row2", 2.0]],
        columns=["Non-Nullable String", "Non-Nullable Float"],
    )
    expected["Non-Nullable String"] = expected["Non-Nullable String"].astype("string")

    tm.assert_frame_equal(result, expected)


def test_read_query(df, tmp_hyper):
    pantab.frame_to_hyper(df, tmp_hyper, table="test")

    query = "SELECT int16 AS i, '_' || int32 AS _i2 FROM test"
    result = pantab.frame_from_hyper_query(tmp_hyper, query)

    expected = pd.DataFrame([[1, "_2"], [6, "_7"], [0, "_0"]], columns=["i", "_i2"])
    expected = expected.astype({"i": "Int16", "_i2": "string"})

    tm.assert_frame_equal(result, expected)


def test_empty_read_query(df: pd.DataFrame, tmp_hyper):
    """
    red-green for empty query results
    """
    # sql cols need to base case insensitive & unique
    df = df[pd.Series(df.columns).apply(lambda s: s.lower()).drop_duplicates()]
    table_name = "test"
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name)
    query = f"SELECT * FROM {table_name} limit 0"
    expected = pd.DataFrame(columns=df.columns).astype(df.dtypes)

    # read_query does not maintain nullability
    # and not all types roundtrip properly
    expected = expected.astype(
        {
            "int16": "Int16",
            "int32": "Int32",
            "int64": "Int64",
            "int16_limits": "Int16",
            "int32_limits": "Int32",
            "int64_limits": "Int64",
            "float32": "float64",
            "non-ascii": "string",
            "object": "string",
        }
    )
    result = pantab.frame_from_hyper_query(tmp_hyper, query)
    tm.assert_frame_equal(result, expected)
