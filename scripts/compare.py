"""
Script for evaluating general-purpose performance against hand-written Hyper API code.

Note that the fastest way to load to the Hyper API is using their CSV command, which are
not covered here. So these benchmarks are arguably unfair, but on the flip side these
are probably more reflective of how a Python programmer would expect to code.

The Tableau code from benchmarking was taken from an example file in the Hyper API
examples folder. The original copyright has been retained below for reference.
"""

# Start code modified from Tableau sample file

# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------
import time
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pantab
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    CreateMode,
    NOT_NULLABLE,
    NULLABLE,
    SqlType,
    TableDefinition,
    Inserter,
    escape_name,
    escape_string_literal,
    TableName,
    HyperException,
)

# The table is called "Extract" and will be created in the "Extract" schema.
# This has historically been the default table name and schema for extracts created by Tableau
extract_table = TableDefinition(
    table_name=TableName("Extract", "Extract"),
    columns=[
        TableDefinition.Column(
            name="a", type=SqlType.big_int(), nullability=NOT_NULLABLE
        ),
        TableDefinition.Column(
            name="b", type=SqlType.big_int(), nullability=NOT_NULLABLE
        ),
        TableDefinition.Column(
            name="c", type=SqlType.big_int(), nullability=NOT_NULLABLE
        ),
        TableDefinition.Column(
            name="d", type=SqlType.big_int(), nullability=NOT_NULLABLE
        ),
        TableDefinition.Column(
            name="e", type=SqlType.big_int(), nullability=NOT_NULLABLE
        ),
    ],
)


def run_insert_data_into_single_table(nrows: int) -> int:
    """
    An example demonstrating a simple single-table Hyper file including table 
    creation and data insertion with different types
    """
    path_to_database = Path("customer.hyper")

    data_to_insert: List[List[int]] = []
    for _ in range(nrows):
        data_to_insert.append([1, 1, 1, 1, 1])

    start = time.time()
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        with Connection(
            endpoint=hyper.endpoint,
            database=path_to_database,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:

            connection.catalog.create_schema(
                schema=extract_table.table_name.schema_name
            )
            connection.catalog.create_table(table_definition=extract_table)

            with Inserter(connection, extract_table) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()

    end = time.time()

    return end - start


# End code modified from Tableau sample file
def time_from_pantab(nrows: int) -> int:
    df = pd.DataFrame(np.ones((nrows, 5)), columns=list("abcde"))
    start = time.time()
    pantab.frame_to_hyper(df, "customer.hyper", table="extract")
    end = time.time()

    return end - start


if __name__ == "__main__":
    hyper_times: List[float] = []
    pantab_times: List[float] = []

    nrows = (10_000, 50_000, 100_000, 500_000, 1_000_000, 10_000_000)
    for x in nrows:
        hyper_times.append(run_insert_data_into_single_table(x))
        pantab_times.append(time_from_pantab(x))

    df = pd.DataFrame({"pantab": pantab_times, "Hyper API": hyper_times}, index=nrows)
    df.plot(kind="bar",)
    plt.tight_layout()
    plt.show()
