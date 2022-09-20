from datetime import (
    date as Date,
    datetime as DateTime,
    time as Time,
    timedelta as TimeDelta,
    timezone as TimeZone,
)
from decimal import Decimal
from ipaddress import IPv4Address, IPv4Network

import pytest

from pg8000.converters import (
    PGInterval,
    PY_TYPES,
    array_out,
    array_string_escape,
    date_in,
    identifier,
    interval_in,
    literal,
    make_param,
    null_out,
    numeric_in,
    numeric_out,
    pg_interval_in,
    string_in,
    string_out,
    timestamptz_in,
)
from pg8000.native import InterfaceError


@pytest.mark.parametrize(
    "value,expected",
    [
        ["2022-03-02", Date(2022, 3, 2)],
        ["infinity", "infinity"],
        ["-infinity", "-infinity"],
    ],
)
def test_date_in(value, expected):
    assert date_in(value) == expected


def test_null_out():
    assert null_out(None) is None


@pytest.mark.parametrize(
    "array,out",
    [
        [[True, False, None], "{true,false,NULL}"],  # bool[]
        [[IPv4Address("192.168.0.1")], "{192.168.0.1}"],  # inet[]
        [[Date(2021, 3, 1)], "{2021-03-01}"],  # date[]
        [[b"\x00\x01\x02\x03\x02\x01\x00"], '{"\\\\x00010203020100"}'],  # bytea[]
        [[IPv4Network("192.168.0.0/28")], "{192.168.0.0/28}"],  # inet[]
        [[1, 2, 3], "{1,2,3}"],  # int2[]
        [[1, None, 3], "{1,NULL,3}"],  # int2[] with None
        [[[1, 2], [3, 4]], "{{1,2},{3,4}}"],  # int2[] multidimensional
        [[70000, 2, 3], "{70000,2,3}"],  # int4[]
        [[7000000000, 2, 3], "{7000000000,2,3}"],  # int8[]
        [[0, 7000000000, 2], "{0,7000000000,2}"],  # int8[]
        [[1.1, 2.2, 3.3], "{1.1,2.2,3.3}"],  # float8[]
        [["Veni", "vidi", "vici"], "{Veni,vidi,vici}"],  # varchar[]
    ],
)
def test_array_out(array, out):
    assert array_out(array) == out


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
    ],
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
    ],
)
def test_numeric_in(value):
    assert numeric_in(value) == Decimal(value)


@pytest.mark.parametrize(
    "value",
    [
        "hello \u0173 world",
    ],
)
def test_string_out(value):
    assert string_out(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "hello \u0173 world",
    ],
)
def test_string_in(value):
    assert string_in(value) == value


def test_PGInterval_init():
    i = PGInterval(days=1)
    assert i.months is None
    assert i.days == 1
    assert i.microseconds is None


def test_PGInterval_repr():
    v = PGInterval(microseconds=123456789, days=2, months=24)
    assert repr(v) == "<PGInterval 24 months 2 days 123456789 microseconds>"


def test_PGInterval_str():
    v = PGInterval(microseconds=123456789, days=2, months=24, millennia=2)
    assert str(v) == "2 millennia 24 months 2 days 123456789 microseconds"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("P1Y2M", PGInterval(years=1, months=2)),
        ("P12DT30S", PGInterval(days=12, seconds=30)),
        (
            "P-1Y-2M3DT-4H-5M-6S",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ),
        ("PT1M32.32S", PGInterval(minutes=1, seconds=32.32)),
    ],
)
def test_PGInterval_from_str_iso_8601(value, expected):
    interval = PGInterval.from_str_iso_8601(value)
    assert interval == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("@ 1 year 2 mons", PGInterval(years=1, months=2)),
        (
            "@ 3 days 4 hours 5 mins 6 secs",
            PGInterval(days=3, hours=4, minutes=5, seconds=6),
        ),
        (
            "@ 1 year 2 mons -3 days 4 hours 5 mins 6 secs ago",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ),
    ],
)
def test_PGInterval_from_str_postgres(value, expected):
    interval = PGInterval.from_str_postgres(value)
    assert interval == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ["1-2", PGInterval(years=1, months=2)],
        ["3 4:05:06", PGInterval(days=3, hours=4, minutes=5, seconds=6)],
        [
            "-1-2 +3 -4:05:06",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ],
        ["8 4:00:32.32", PGInterval(days=8, hours=4, minutes=0, seconds=32.32)],
    ],
)
def test_PGInterval_from_str_sql_standard(value, expected):
    interval = PGInterval.from_str_sql_standard(value)
    assert interval == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("P12DT30S", PGInterval(days=12, seconds=30)),
        ("@ 1 year 2 mons", PGInterval(years=1, months=2)),
        ("1-2", PGInterval(years=1, months=2)),
        ("3 4:05:06", PGInterval(days=3, hours=4, minutes=5, seconds=6)),
        (
            "-1-2 +3 -4:05:06",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ),
        ("00:00:30", PGInterval(seconds=30)),
    ],
)
def test_PGInterval_from_str(value, expected):
    interval = PGInterval.from_str(value)
    assert interval == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1 year", PGInterval(years=1)),
        ("2 hours", PGInterval(hours=2)),
    ],
)
def test_pg_interval_in(value, expected):
    assert pg_interval_in(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2 hours", TimeDelta(hours=2)),
        ("00:00:30", TimeDelta(seconds=30)),
    ],
)
def test_interval_in(value, expected):
    assert interval_in(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("a", "a"),
        ('"', '"\\""'),
        ("\r", '"\r"'),
        ("\n", '"\n"'),
        ("\t", '"\t"'),
    ],
)
def test_array_string_escape(value, expected):
    res = array_string_escape(value)
    assert res == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        [
            "2022-10-08 15:01:39+01:30",
            DateTime(
                2022, 10, 8, 15, 1, 39, tzinfo=TimeZone(TimeDelta(hours=1, minutes=30))
            ),
        ],
        [
            "2022-10-08 15:01:39-01:30",
            DateTime(
                2022,
                10,
                8,
                15,
                1,
                39,
                tzinfo=TimeZone(TimeDelta(hours=-1, minutes=-30)),
            ),
        ],
        [
            "2022-10-08 15:01:39+02",
            DateTime(2022, 10, 8, 15, 1, 39, tzinfo=TimeZone(TimeDelta(hours=2))),
        ],
        [
            "2022-10-08 15:01:39-02",
            DateTime(2022, 10, 8, 15, 1, 39, tzinfo=TimeZone(TimeDelta(hours=-2))),
        ],
        [
            "2022-10-08 15:01:39.597026+01:30",
            DateTime(
                2022,
                10,
                8,
                15,
                1,
                39,
                597026,
                tzinfo=TimeZone(TimeDelta(hours=1, minutes=30)),
            ),
        ],
        [
            "2022-10-08 15:01:39.597026-01:30",
            DateTime(
                2022,
                10,
                8,
                15,
                1,
                39,
                597026,
                tzinfo=TimeZone(TimeDelta(hours=-1, minutes=-30)),
            ),
        ],
        [
            "2022-10-08 15:01:39.597026+02",
            DateTime(
                2022, 10, 8, 15, 1, 39, 597026, tzinfo=TimeZone(TimeDelta(hours=2))
            ),
        ],
        [
            "2022-10-08 15:01:39.597026-02",
            DateTime(
                2022, 10, 8, 15, 1, 39, 597026, tzinfo=TimeZone(TimeDelta(hours=-2))
            ),
        ],
    ],
)
def test_timestamptz_in(value, expected):
    assert timestamptz_in(value) == expected


def test_make_param():
    class BClass(bytearray):
        pass

    val = BClass(b"\x00\x01\x02\x03\x02\x01\x00")
    assert make_param(PY_TYPES, val) == "\\x00010203020100"


def test_identifier_int():
    with pytest.raises(InterfaceError, match="identifier must be a str"):
        identifier(9)


def test_identifier_empty():
    with pytest.raises(
        InterfaceError, match="identifier must be > 0 characters in length"
    ):
        identifier("")


def test_identifier_quoted_null():
    with pytest.raises(
        InterfaceError, match="identifier cannot contain the code zero character"
    ):
        identifier("tabl\u0000e")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("top_secret", "top_secret"),
        (" Table", '" Table"'),
        ("A Table", '"A Table"'),
        ('A " Table', '"A "" Table"'),
        ("Table$", "Table$"),
    ],
)
def test_identifier_success(value, expected):
    assert identifier(value) == expected


def test_literal():
    val = "top_secret"
    assert literal(val) == f"'{val}'"


def test_literal_quote():
    assert literal("bob's") == "'bob''s'"


def test_literal_int():
    assert literal(7) == "7"


def test_literal_float():
    assert literal(7.9) == "7.9"


def test_literal_decimal():
    assert literal(Decimal("0.1")) == "0.1"


def test_literal_bytes():
    assert literal(b"\x03") == "X'03'"


def test_literal_boolean():
    assert literal(True) == "TRUE"


def test_literal_None():
    assert literal(None) == "NULL"


def test_literal_Time():
    assert literal(Time(22, 13, 2)) == "'22:13:02'"


def test_literal_Date():
    assert literal(Date(2063, 11, 2)) == "'2063-11-02'"


def test_literal_TimeDelta():
    assert literal(TimeDelta(22, 13, 2)) == "'22 days 13 seconds 2 microseconds'"


def test_literal_Datetime():
    assert literal(DateTime(2063, 3, 31, 22, 13, 2)) == "'2063-03-31T22:13:02'"


def test_literal_Trojan():
    class Trojan:
        def __str__(self):
            return "A Gift"

    assert literal(Trojan()) == "'A Gift'"
