import numpy as np
import pandas as pd
import pandas.util.testing as tm
import tableauhyperapi as tab_api

import pantab


def test_basic(df, tmp_hyper, table_name, table_mode):
    # Write twice; depending on mode this should either overwrite or duplicate entries
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)

    expected = df.copy()

    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    expected["float32"] = expected["float32"].astype(np.float64)

    tm.assert_frame_equal(result, expected)


def test_missing_data(tmp_hyper, table_name, table_mode):
    df = pd.DataFrame([[np.nan], [1]], columns=list("a"))
    df["b"] = pd.Series([None, np.nan], dtype=object)  # no inference
    df["c"] = pd.Series([np.nan, "c"])

    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)
    pantab.frame_to_hyper(df, tmp_hyper, table=table_name, table_mode=table_mode)

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    expected = pd.DataFrame(
        [[np.nan, np.nan, np.nan], [1, np.nan, "c"]], columns=list("abc")
    )
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    tm.assert_frame_equal(result, expected)


def test_insert_data(rnd_df, tmp_hyper, table_name, update_mode):
    # Write partial DataFrame to act as a preexisting table.
    pantab.frame_to_hyper(rnd_df.loc[0:10], tmp_hyper, table=table_name)

    # Insert 'new' records.
    pantab.frame_to_hyper(
        rnd_df, tmp_hyper, table=table_name, table_mode=update_mode, table_key="keys",
    )

    result = pantab.frame_from_hyper(tmp_hyper, table=table_name)
    expected = rnd_df

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

    expected = df.copy()
    if table_mode == "a":
        expected = pd.concat([expected, expected]).reset_index(drop=True)

    expected["float32"] = expected["float32"].astype(np.float64)

    # some test trickery here
    if not isinstance(table_name, tab_api.TableName) or table_name.schema_name is None:
        table_name = tab_api.TableName("public", table_name)

    assert set(result.keys()) == set(
        (table_name, tab_api.TableName("public", "table2"))
    )
    for val in result.values():
        tm.assert_frame_equal(val, expected)


def test_multi_table_insert(rnd_df, tmp_hyper, table_name, update_mode):
    # Write partial DataFrames to act as preexisting tables.
    df1, df2 = (rnd_df.loc[0:1], rnd_df.loc[0:25])

    pantab.frames_to_hyper(
        {table_name: df1, "table2": df2},
        list_of_keys=["keys", "keys"],
        database=tmp_hyper,
    )

    # Perform insert of 'new' records.
    pantab.frames_to_hyper(
        {table_name: rnd_df, "table2": rnd_df},
        list_of_keys=["keys", "keys"],
        database=tmp_hyper,
        table_mode=update_mode,
    )

    expected = rnd_df
    result = pantab.frames_from_hyper(tmp_hyper)

    for val in result.values():
        tm.assert_frame_equal(val, expected)
