import pytest

from pg8000.types import PGInterval, Range


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
        (
            "@ 1 millennium -2 mons",
            PGInterval(millennia=1, months=-2),
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


def test_Range_equals():
    pg_range_a = Range("[", 1, 2, ")")
    pg_range_b = Range("[", 1, 2, ")")
    assert pg_range_a == pg_range_b


def test_Range_str():
    v = Range(5, 6)
    assert str(v) == "[5,6)"
