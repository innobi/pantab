import numpy as np
import pandas as pd
from tableauhyperapi import HyperProcess, Telemetry

import pantab


class TimeSuite:
    def setup(self):
        nrows = 10_000
        data = [
            [
                1,
                2,
                3,
                1,
                2,
                3,
                4.0,
                5.0,
                True,
                pd.to_datetime("2018-01-01"),
                pd.to_datetime("2018-01-01", utc=True),
                pd.Timedelta("1 days 2 hours 3 minutes 4 seconds"),
                "foo",
            ]
        ] * nrows

        df = pd.DataFrame(
            data,
            columns=[
                "int16",
                "int32",
                "int64",
                "Int16",
                "Int32",
                "Int64",
                "float32",
                "float64",
                "bool",
                "datetime64",
                "datetime64_utc",
                "timedelta64",
                "object",
            ],
        )

        self.df = df.astype(
            {
                "int16": np.int16,
                "int32": np.int32,
                "int64": np.int64,
                "float32": np.float32,
                "float64": np.float64,
                "bool": np.bool,
                "datetime64": "datetime64[ns]",
                "datetime64_utc": "datetime64[ns, UTC]",
                "timedelta64": "timedelta64[ns]",
                "object": "object",
            }
        )

        path = "test.hyper"
        pantab.frame_to_hyper(df, path, table="test")
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)

        return df

    def teardown(self):
        self.hyper.close()

    def time_write_frame(self):
        pantab.frame_to_hyper(
            self.df, "dummy.hyper", table="dummy", hyper_process=self.hyper
        )

    def time_read_frame(self):
        pantab.frame_from_hyper("test.hyper", table="test", hyper_process=self.hyper)


class TimeWriteLong:
    def setup(self):
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.df = pd.DataFrame(np.ones((10_000_000, 1)), columns=["a"])

    def teardown(self):
        self.hyper.close()

    def time_write_frame(self):
        pantab.frame_to_hyper(
            self.df, "dummy.hyper", table="dummy", hyper_process=self.hyper
        )

    def peakmem_write_frame(self):
        pantab.frame_to_hyper(
            self.df, "dummy.hyper", table="dummy", hyper_process=self.hyper
        )


class TimeReadLong:
    def setup(self):
        df = pd.DataFrame(np.ones((10_000_000, 1)), columns=["a"])
        path = "test.hyper"
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        pantab.frame_to_hyper(df, path, table="test", hyper_process=self.hyper)

    def teardown(self):
        self.hyper.close()

    def time_read_frame(self):
        pantab.frame_from_hyper("test.hyper", table="test", hyper_process=self.hyper)

    def peakmem_read_frame(self):
        pantab.frame_from_hyper("test.hyper", table="test", hyper_process=self.hyper)
