import sys

import pyarrow as pa
import pytest

import pantab as pt


def test_decimal_roundtrip(tmp_hyper, compat):
    schema = pa.schema(
        [
            ("no_fractional", pa.decimal128(38, 0)),
            ("mixed_decimal", pa.decimal128(38, 10)),
            ("not_using_entire_precision", pa.decimal128(38, 10)),
            ("only_fractional", pa.decimal128(38, 38)),
            ("five_two", pa.decimal128(5, 2)),
        ]
    )

    tbl = pa.Table.from_arrays(
        [
            pa.array(
                [
                    "12345678901234567890123456789012345678",
                    "98765432109876543210987654321098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "1234567890123456789012345678.9012345678",
                    "9876543210987654321098765432.1098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "123.45",
                    "543.21",
                    None,
                ]
            ),
            pa.array(
                [
                    ".12345678901234567890123456789012345678",
                    ".98765432109876543210987654321098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "123.45",
                    "543.21",
                    None,
                ]
            ),
        ],
        schema=schema,
    )

    pt.frame_to_hyper(
        tbl,
        tmp_hyper,
        table="decimals",
        process_params={"default_database_version": "3"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table="decimals", return_type="pyarrow")
    expected = tbl

    compat.assert_frame_equal(result, expected)


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="negative decimals broken on windows"
)
def test_decimal_negative(tmp_hyper, compat):
    # there is a nanoarrow or arrow bug where negative values on windows
    # are not working. See nanoarrow GH issue 594
    schema = pa.schema(
        [
            ("no_fractional", pa.decimal128(38, 0)),
            ("mixed_decimal", pa.decimal128(38, 10)),
            ("not_using_entire_precision", pa.decimal128(38, 10)),
            ("only_fractional", pa.decimal128(38, 38)),
            ("five_two", pa.decimal128(5, 2)),
        ]
    )

    tbl = pa.Table.from_arrays(
        [
            pa.array(
                [
                    "-12345678901234567890123456789012345678",
                    "-98765432109876543210987654321098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "-1234567890123456789012345678.9012345678",
                    "-9876543210987654321098765432.1098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "-123.45",
                    "-543.21",
                    None,
                ]
            ),
            pa.array(
                [
                    "-.12345678901234567890123456789012345678",
                    "-.98765432109876543210987654321098765432",
                    None,
                ]
            ),
            pa.array(
                [
                    "-123.45",
                    "-543.21",
                    None,
                ]
            ),
        ],
        schema=schema,
    )

    pt.frame_to_hyper(
        tbl,
        tmp_hyper,
        table="decimals",
        process_params={"default_database_version": "3"},
    )

    result = pt.frame_from_hyper(tmp_hyper, table="decimals", return_type="pyarrow")
    expected = tbl

    compat.assert_frame_equal(result, expected)
