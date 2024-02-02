import pathlib
from sqlite3 import connect

import pandas as pd
import pandas.testing as tm
import pytest
from tableauhyperapi import HyperProcess, TableName, Telemetry

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
    with pytest.raises(
        TypeError, match=r"Column \"x\" has unsupported datatype GEOGRAPHY"
    ):
        pantab.frame_from_hyper(db_path, table="test")


def test_months_in_interval_raises(df, tmp_hyper, monkeypatch):
    # Monkeypatch a new constructor that hard codes months
    def __init__(self, months: int, days: int, microseconds: int):
        self.months = 1
        self.days = days
        self.microseconds = microseconds

    monkeypatch.setattr(pantab._writer.tab_api.Interval, "__init__", __init__)
    pantab.frame_to_hyper(df, tmp_hyper, table="test")
    with pytest.raises(
        ValueError, match=r"Cannot read Intervals with month components\."
    ):
        pantab.frame_from_hyper(tmp_hyper, table="test")

    with pytest.raises(
        ValueError, match=r"Cannot read Intervals with month components\."
    ):
        pantab.frames_from_hyper(tmp_hyper)


def test_error_on_first_column(df, tmp_hyper, monkeypatch):
    """
    We had a defect due to which pantab segfaulted when an error occured in one of
    the first two columns. This test case is a regression test against that.
    """

    # Monkeypatch a new constructor that hard codes months
    def __init__(self, months: int, days: int, microseconds: int):
        self.months = 1
        self.days = days
        self.microseconds = microseconds

    monkeypatch.setattr(pantab._writer.tab_api.Interval, "__init__", __init__)

    df = pd.DataFrame(
        [[pd.Timedelta("1 days 2 hours 3 minutes 4 seconds")]], columns=["timedelta64"]
    ).astype({"timedelta64": "timedelta64[ns]"})
    with pytest.warns(DeprecationWarning, match=r"removed in pantab 4.0"):
        pantab.frame_to_hyper(df, tmp_hyper, table="test")

    with pytest.raises(
        ValueError, match=r"Cannot read Intervals with month components\."
    ), pytest.warns(DeprecationWarning, match=r"removed in pantab 4.0"):
        pantab.frame_from_hyper(tmp_hyper, table="test")


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
    conn = connect(":memory:")
    table_name = "test"
    df.to_sql(name=table_name, con=conn, index=False)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name)
    query = f"SELECT * FROM {table_name} limit 0"
    expected = pd.read_sql_query(query, conn)
    result = pantab.frame_from_hyper_query(tmp_hyper, query)
    tm.assert_frame_equal(result, expected)


def test_read_with_external_hyper_process_warns(df, tmp_hyper):
    pantab.frame_to_hyper(df, tmp_hyper, table="test")

    default_log_path = pathlib.Path.cwd() / "hyperd.log"
    if default_log_path.exists():
        default_log_path.unlink()

    # By passing in a pre-spawned HyperProcess, one can e.g. avoid creating a log file
    parameters = {"log_config": ""}
    with HyperProcess(
        Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=parameters
    ) as hyper:
        # test frame_to_hyper/frame_from_hyper
        with pytest.warns(DeprecationWarning, match=r"removed in pantab 4.0"):
            pantab.frame_from_hyper(tmp_hyper, table="test", hyper_process=hyper)
