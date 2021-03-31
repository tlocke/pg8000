from datetime import date as Date
from decimal import Decimal
from ipaddress import IPv4Address, IPv4Network

from pg8000.converters import (
    BIGINT_ARRAY, BOOLEAN_ARRAY, BYTES_ARRAY, DATE_ARRAY, FLOAT_ARRAY,
    INET_ARRAY, INTEGER_ARRAY, SMALLINT_ARRAY, VARCHAR_ARRAY, array_inspect,
    null_out, numeric_in, numeric_out, string_in, string_out
)

import pytest


def test_null_out():
    assert null_out(None) is None


@pytest.mark.parametrize(
    "array,oid",
    [
        [[True, False, None], BOOLEAN_ARRAY],  # bool[]
        [[IPv4Address('192.168.0.1')], INET_ARRAY],  # inet[]
        [[Date(2021, 3, 1)], DATE_ARRAY],  # date[]
        [[b'\x00\x01\x02\x03\x02\x01\x00'], BYTES_ARRAY],  # bytea[]
        [[IPv4Network('192.168.0.0/28')], INET_ARRAY],  # inet[]
        [[1, 2, 3], SMALLINT_ARRAY],  # int2[]
        [[1, None, 3], SMALLINT_ARRAY],  # int2[] with None
        [[[1, 2], [3, 4]], SMALLINT_ARRAY],  # int2[] multidimensional
        [[70000, 2, 3], INTEGER_ARRAY],  # int4[]
        [[7000000000, 2, 3], BIGINT_ARRAY],  # int8[]
        [[1.1, 2.2, 3.3], FLOAT_ARRAY],  # float8[]
        [["Veni", "vidi", "vici"], VARCHAR_ARRAY],  # varchar[]
    ]
)
def test_array_inspect(con, array, oid):
    array_oid, _ = array_inspect(array)
    assert array_oid == oid


@pytest.mark.parametrize(
    "value",
    [
        "1.1",
        "-1.1",
        "10000",
        "20000",
        "-1000000000.123456789",
        "1.0",
        "12.44",
    ]
)
def test_numeric_out(value):
    assert numeric_out(value) == str(value)


@pytest.mark.parametrize(
    "value",
    [
        "1.1",
        "-1.1",
        "10000",
        "20000",
        "-1000000000.123456789",
        "1.0",
        "12.44",
    ]
)
def test_numeric_in(value):
    assert numeric_in(value) == Decimal(value)


@pytest.mark.parametrize(
    "value",
    [
        "hello \u0173 world",
    ]
)
def test_string_out(value):
    assert string_out(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "hello \u0173 world",
    ]
)
def test_string_in(value):
    assert string_in(value) == value
