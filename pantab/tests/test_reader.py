import pytest

import pantab


def test_read_doesnt_modify_existing_file(df, tmp_hyper):
    pantab.frame_to_hyper(df, tmp_hyper, table="test")
    last_modified = tmp_hyper.stat().st_mtime

    # Try out our read methods
    pantab.frame_from_hyper(tmp_hyper, table="test")
    pantab.frames_from_hyper(tmp_hyper)

    # Neither should not update file stats
    assert last_modified == tmp_hyper.stat().st_mtime


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
