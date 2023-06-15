import os
import time
from collections import OrderedDict
from datetime import (
    date as Date,
    datetime as Datetime,
    time as Time,
    timedelta as Timedelta,
    timezone as Timezone,
)
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address, IPv4Network
from json import dumps
from locale import LC_ALL, localeconv, setlocale
from uuid import UUID

import pytest

import pytz

from pg8000.converters import (
    BIGINT,
    BIGINT_ARRAY,
    BOOLEAN,
    CIDR_ARRAY,
    DATE,
    FLOAT_ARRAY,
    INET,
    INTEGER_ARRAY,
    INTERVAL,
    JSON,
    JSONB,
    JSONB_ARRAY,
    JSON_ARRAY,
    MONEY,
    MONEY_ARRAY,
    NUMERIC,
    NUMERIC_ARRAY,
    PGInterval,
    POINT,
    Range,
    SMALLINT_ARRAY,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    TIMESTAMPTZ_ARRAY,
    TIMESTAMP_ARRAY,
    UUID_ARRAY,
    UUID_TYPE,
    XID,
    pg_interval_in,
    pg_interval_out,
)


def test_str_then_int(con):
    v1 = "hello world"
    retval = con.run("SELECT cast(:v1 as varchar) as f1", v1=v1)
    assert retval[0][0] == v1

    v2 = 1
    retval = con.run("SELECT cast(:v2 as varchar) as f1", v2=v2)
    assert retval[0][0] == str(v2)


def test_insert_null(con):
    v = None
    con.run("CREATE TEMPORARY TABLE test_int (f INTEGER)")
    con.run("INSERT INTO test_int VALUES (:v)", v=v)
    retval = con.run("SELECT * FROM test_int")
    assert retval[0][0] == v


def test_int_roundtrip(con):
    int2 = 21
    int4 = 23
    int8 = 20

    MAP = {
        int2: "int2",
        int4: "int4",
        int8: "int8",
    }

    test_values = [
        (0, int2),
        (-32767, int2),
        (-32768, int4),
        (+32767, int2),
        (+32768, int4),
        (-2147483647, int4),
        (-2147483648, int8),
        (+2147483647, int4),
        (+2147483648, int8),
        (-9223372036854775807, int8),
        (+9223372036854775807, int8),
    ]

    for value, typoid in test_values:
        retval = con.run("SELECT cast(:v as " + MAP[typoid] + ")", v=value)
        assert retval[0][0] == value
        column_type_oid = con.columns[0]["type_oid"]
        assert column_type_oid == typoid


def test_bytearray_subclass_round_trip(con):
    class BClass(bytearray):
        pass

    binary = b"\x00\x01\x02\x03\x02\x01\x00"
    retval = con.run("SELECT cast(:v as bytea)", v=BClass(binary))
    assert retval[0][0] == binary


def test_timestamp_roundtrip(con):
    v = Datetime(2001, 2, 3, 4, 5, 6, 170000)
    retval = con.run("SELECT cast(:v as timestamp)", v=v)
    assert retval[0][0] == v

    # Test that time zone doesn't affect it
    orig_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/Edmonton"
    time.tzset()

    retval = con.run("SELECT cast(:v as timestamp)", v=v)
    assert retval[0][0] == v

    if orig_tz is None:
        del os.environ["TZ"]
    else:
        os.environ["TZ"] = orig_tz
    time.tzset()


def test_interval_roundtrip(con):
    con.register_in_adapter(INTERVAL, pg_interval_in)
    con.register_out_adapter(PGInterval, pg_interval_out)
    v = PGInterval(microseconds=123456789, days=2, months=24)
    retval = con.run("SELECT cast(:v as interval)", v=v)
    assert retval[0][0] == v


def test_enum_str_round_trip(con):
    try:
        con.run("create type lepton as enum ('electron', 'muon', 'tau')")

        v = "muon"
        retval = con.run("SELECT cast(:v as lepton) as f1", v=v)
        assert retval[0][0] == v
        con.run("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        con.run("INSERT INTO testenum VALUES (cast(:v as lepton))", v="electron")
    finally:
        con.run("drop table testenum")
        con.run("drop type lepton")


def test_enum_custom_round_trip(con):
    class Lepton:
        # Implements PEP 435 in the minimal fashion needed
        __members__ = OrderedDict()

        def __init__(self, name, value, alias=None):
            self.name = name
            self.value = value
            self.__members__[name] = self
            setattr(self.__class__, name, self)
            if alias:
                self.__members__[alias] = self
                setattr(self.__class__, alias, self)

    def lepton_out(lepton):
        return lepton.value

    try:
        con.run("create type lepton as enum ('1', '2', '3')")
        con.register_out_adapter(Lepton, lepton_out)

        v = Lepton("muon", "2")
        retval = con.run("SELECT CAST(:v AS lepton)", v=v)
        assert retval[0][0] == v.value
    finally:
        con.run("drop type lepton")


def test_enum_py_round_trip(con):
    class Lepton(Enum):
        electron = "1"
        muon = "2"
        tau = "3"

    try:
        con.run("create type lepton as enum ('1', '2', '3')")

        v = Lepton.muon
        retval = con.run("SELECT cast(:v as lepton) as f1", v=v)
        assert retval[0][0] == v.value

        con.run("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        con.run("INSERT INTO testenum VALUES (cast(:v as lepton))", v=Lepton.electron)
    finally:
        con.run("drop table testenum")
        con.run("drop type lepton")


def test_xml_roundtrip(con):
    v = "<genome>gatccgagtac</genome>"
    retval = con.run("select xmlparse(content :v) as f1", v=v)
    assert retval[0][0] == v


def test_int2vector_in(con):
    retval = con.run("select cast('1 2' as int2vector) as f1")
    assert retval[0][0] == [1, 2]

    # Should complete without an exception
    con.run("select indkey from pg_index")


@pytest.mark.parametrize(
    "tz, test_input,test_output",
    [
        [
            "UTC",
            "2001-02-03 04:05:06.17 America/Edmonton",
            Datetime(2001, 2, 3, 11, 5, 6, 170000, Timezone.utc),
        ],
        [
            "UTC",
            "2001-02-03 04:05:06.17+01:30",
            Datetime(2001, 2, 3, 2, 35, 6, 170000, Timezone.utc),
        ],
        [
            "01:30",
            "2001-02-03 04:05:06.17+01:30",
            Datetime(2001, 2, 3, 2, 35, 6, 170000, Timezone.utc),
        ],
        [
            "UTC",
            "infinity",
            "infinity",
        ],
        [
            "UTC",
            "-infinity",
            "-infinity",
        ],
    ],
)
def test_timestamptz_in(con, tz, test_input, test_output):
    con.run(f"SET TIME ZONE '{tz}'")
    retval = con.run(f"SELECT CAST('{test_input}' AS timestamp with time zone)")
    dt = retval[0][0]

    assert dt == test_output


def test_timestamp_tz_roundtrip(con):
    mst = pytz.timezone("America/Edmonton")
    v1 = mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000))
    retval = con.run("SELECT cast(:v as timestamptz)", v=v1)
    v2 = retval[0][0]
    assert v2.tzinfo is not None
    assert v1 == v2


def test_timestamp_mismatch(con):
    mst = pytz.timezone("America/Edmonton")
    con.run("SET SESSION TIME ZONE 'America/Edmonton'")
    try:
        con.run(
            "CREATE TEMPORARY TABLE TestTz (f1 timestamp with time zone, "
            "f2 timestamp without time zone)"
        )
        con.run(
            "INSERT INTO TestTz (f1, f2) VALUES (:v1, :v2)",
            # insert timestamp into timestamptz field (v1)
            v1=Datetime(2001, 2, 3, 4, 5, 6, 170000),
            # insert timestamptz into timestamp field (v2)
            v2=mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000)),
        )
        retval = con.run("SELECT f1, f2 FROM TestTz")

        # when inserting a timestamp into a timestamptz field,
        # postgresql assumes that it is in local time. So the value
        # that comes out will be the server's local time interpretation
        # of v1. We've set the server's TZ to MST, the time should
        # be...
        f1 = retval[0][0]
        assert f1 == Datetime(2001, 2, 3, 11, 5, 6, 170000, Timezone.utc)

        # inserting the timestamptz into a timestamp field, pg8000 converts the
        # value into UTC, and then the PG server sends that time back
        f2 = retval[0][1]
        assert f2 == Datetime(2001, 2, 3, 11, 5, 6, 170000)
    finally:
        con.run("SET SESSION TIME ZONE DEFAULT")


@pytest.mark.parametrize(
    "select,expected",
    [
        ["CAST('t' AS bool)", True],
        ["5000::smallint", 5000],
        ["5000::numeric", Decimal("5000")],
        ["50.34::numeric", Decimal("50.34")],
        ["5000::integer", 5000],
        ["50000000000000::bigint", 50000000000000],
        ["1.1::real", 1.1],
        ["1.1::double precision", 1.1000000000000001],
        ["'hello'::varchar(20)", "hello"],
        ["'hello'::char(20)", "hello               "],
        ["'hello'::text", "hello"],
        ["(1,2)", ("1", "2")],
    ],
)
def test_in(con, select, expected):
    retval = con.run(f"SELECT {select}")
    assert retval[0][0] == expected


def test_name_out(con):
    # select a field that is of "name" type:
    con.run("SELECT usename FROM pg_user")
    # It is sufficient that no errors were encountered.


def test_oid_out(con):
    con.run("SELECT oid FROM pg_type")
    # It is sufficient that no errors were encountered.


def test_pg_interval_in(con):
    con.register_in_adapter(1186, pg_interval_in)
    retval = con.run(
        "SELECT CAST('1 month 16 days 12 hours 32 minutes 64 seconds' as INTERVAL)"
    )
    expected_value = PGInterval(
        microseconds=(12 * 60 * 60 * 1000 * 1000)
        + (32 * 60 * 1000 * 1000)
        + (64 * 1000 * 1000),
        days=16,
        months=1,
    )
    assert retval[0][0] == expected_value


@pytest.mark.parametrize(
    "test_input,test_output",
    [
        ["12 days 30 seconds", Timedelta(days=12, seconds=30)],
        ["30 seconds", Timedelta(seconds=30)],
    ],
)
def test_interval_in_postgres(con, test_input, test_output):
    con.run("SET intervalstyle TO 'postgres'")
    retval = con.run(f"SELECT CAST('{test_input}' AS INTERVAL)")
    assert retval[0][0] == test_output


@pytest.mark.parametrize(
    "iso_8601,output",
    [
        ["P12DT30S", Timedelta(days=12, seconds=30)],
        ["PT30S", Timedelta(seconds=30)],
        [
            "P-1Y-2M3DT-4H-5M-6S",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ],
    ],
)
def test_interval_in_iso_8601(con, iso_8601, output):
    con.run("SET intervalstyle TO 'iso_8601'")
    retval = con.run(f"SELECT CAST('{iso_8601}' AS INTERVAL)")
    assert retval[0][0] == output


@pytest.mark.parametrize(
    "postgres_verbose,output",
    [
        ["@ 1 year 2 mons", PGInterval(years=1, months=2)],
        [
            "@ 3 days 4 hours 5 mins 6 secs",
            Timedelta(days=3, hours=4, minutes=5, seconds=6),
        ],
        [
            "@ 1 year 2 mons -3 days 4 hours 5 mins 6 secs ago",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ],
    ],
)
def test_interval_in_postgres_verbose(con, postgres_verbose, output):
    con.run("SET intervalstyle TO 'postgres_verbose'")
    retval = con.run(f"SELECT CAST('{postgres_verbose}' AS INTERVAL)")
    assert retval[0][0] == output


@pytest.mark.parametrize(
    "sql_standard,output",
    [
        ["1-2", PGInterval(years=1, months=2)],
        ["3 4:05:06", Timedelta(days=3, hours=4, minutes=5, seconds=6)],
        [
            "-1-2 +3 -4:05:06",
            PGInterval(years=-1, months=-2, days=3, hours=-4, minutes=-5, seconds=-6),
        ],
    ],
)
def test_interval_in_sql_standard(con, sql_standard, output):
    con.run("SET intervalstyle TO 'sql_standard'")
    retval = con.run(f"SELECT CAST('{sql_standard}' AS INTERVAL)")
    assert retval[0][0] == output


def test_timestamp_out(con):
    retval = con.run("SELECT '2001-02-03 04:05:06.17'::timestamp")
    assert retval[0][0] == Datetime(2001, 2, 3, 4, 5, 6, 170000)


def test_int4_array_out(con):
    retval = con.run(
        "SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3"
    )
    f1, f2, f3 = retval[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int2_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::INT2[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT2[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3"
    )
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int8_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::INT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3"
    )
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_bool_array_out(con):
    res = con.run(
        "SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, "
        "'{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, "
        "'{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
        "::BOOL[][][] AS f3"
    )
    f1, f2, f3 = res[0]
    assert f1 == [True, False, False, True]
    assert f2 == [[True, False, True], [False, True, False]]
    assert f3 == [[[True, False], [False, True]], [[None, True], [False, False]]]


def test_float4_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::FLOAT4[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3"
    )
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_float8_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::FLOAT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3"
    )
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


# Find the currency string
setlocale(LC_ALL, "")
CURRENCY = localeconv()["currency_symbol"]
if CURRENCY == "":
    CURRENCY = "$"


@pytest.mark.parametrize(
    "test_input,oid",
    [
        [[Datetime(2001, 2, 3, 4, 5, 6)], TIMESTAMP_ARRAY],  # timestamp[]
        [  # timestamptz[]
            [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc)],
            TIMESTAMPTZ_ARRAY,
        ],
        [
            {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003},
            # json
            JSON,
        ],
        [{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}, JSONB],  # jsonb
        [[IPv4Network("192.168.0.0/28")], CIDR_ARRAY],  # cidr[]
        [[1, 2, 3], SMALLINT_ARRAY],  # int2[]
        [[[1, 2], [3, 4]], SMALLINT_ARRAY],  # int2[] multidimensional
        [[1, None, 3], INTEGER_ARRAY],  # int4[] with None
        [[7000000000, 2, 3], BIGINT_ARRAY],  # int8[]
        [[1.1, 2.2, 3.3], FLOAT_ARRAY],  # float8[]
        [[Decimal("1.1"), None, Decimal("3.3")], NUMERIC_ARRAY],  # numeric[]
        [[f"{CURRENCY}1.10", None, f"{CURRENCY}3.30"], MONEY_ARRAY],  # money[]
        [[UUID("911460f2-1f43-fea2-3e2c-e01fd5b5069d")], UUID_ARRAY],  # uuid[]
        [  # json[]
            [{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}],
            JSON_ARRAY,
        ],
        [  # jsonb[]
            [{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}],
            JSONB_ARRAY,
        ],
        [Time(4, 5, 6), TIME],  # time
        [Date(2001, 2, 3), DATE],  # date
        [Datetime(2001, 2, 3, 4, 5, 6), TIMESTAMP],  # timestamp
        [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc), TIMESTAMPTZ],  # timestamptz
        [True, BOOLEAN],  # bool
        [None, BOOLEAN],  # null
        [Decimal("1.1"), NUMERIC],  # numeric
        [f"{CURRENCY}1.10", MONEY],  # money
        [f"-{CURRENCY}1.10", MONEY],  # money
        [50000000000000, BIGINT],  # int8
        [UUID("911460f2-1f43-fea2-3e2c-e01fd5b5069d"), UUID_TYPE],  # uuid
        [IPv4Network("192.168.0.0/28"), INET],  # inet
        [IPv4Address("192.168.0.1"), INET],  # inet
        [86722, XID],  # xid
        ["infinity", TIMESTAMP],  # timestamp
        [(2.3, 1), POINT],  # point
        [{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}, JSON],  # json
        [{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}, JSONB],  # jsonb
    ],
)
def test_roundtrip_oid(con, test_input, oid):
    retval = con.run("SELECT :v", v=test_input, types={"v": oid})
    assert retval[0][0] == test_input

    assert oid == con.columns[0]["type_oid"]


@pytest.mark.parametrize(
    "test_input,typ,req_ver",
    [
        [[True, False, None], "bool[]", None],
        [
            [
                Range(Date(2023, 6, 1), Date(2023, 6, 6)),
                Range(Date(2023, 6, 10), Date(2023, 6, 13)),
            ],
            "datemultirange",
            14,
        ],
        [Range(Date(1937, 6, 1), Date(2023, 5, 10)), "daterange", None],
        ['"a"=>"1"', "hstore", None],
        [[IPv4Address("192.168.0.1")], "inet[]", None],
        [[Range(3, 7), Range(8, 9)], "int4multirange", 14],
        [Range(3, 7), "int4range", None],
        [50000000000000, "int8", None],
        [[Range(3, 7), Range(8, 9)], "int8multirange", 14],
        [Range(3, 7), "int8range", None],
        [Range(3, 7), "numrange", None],
        [[Range(3, 7), Range(Decimal("9.5"), Decimal("11.4"))], "nummultirange", 14],
        [[Date(2021, 3, 1)], "date[]", None],
        [[Datetime(2001, 2, 3, 4, 5, 6)], "timestamp[]", None],
        [[Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc)], "timestamptz[]", None],
        [[Time(4, 5, 6)], "time[]", None],
        [
            [
                Range(Datetime(2001, 2, 3, 4, 5), Datetime(2023, 2, 3, 4, 5)),
                Range(Datetime(2024, 6, 1), Datetime(2024, 7, 3)),
            ],
            "tsmultirange",
            14,
        ],
        [
            Range(Datetime(2001, 2, 3, 4, 5), Datetime(2023, 2, 3, 4, 5)),
            "tsrange",
            None,
        ],
        [
            [
                Range(
                    Datetime(2001, 2, 3, 4, 5, tzinfo=Timezone.utc),
                    Datetime(2023, 2, 3, 4, 5, tzinfo=Timezone.utc),
                ),
                Range(
                    Datetime(2024, 6, 1, tzinfo=Timezone.utc),
                    Datetime(2024, 7, 3, tzinfo=Timezone.utc),
                ),
            ],
            "tstzmultirange",
            14,
        ],
        [
            Range(
                Datetime(2001, 2, 3, 4, 5, tzinfo=Timezone.utc),
                Datetime(2023, 2, 3, 4, 5, tzinfo=Timezone.utc),
            ),
            "tstzrange",
            None,
        ],
        [[Timedelta(seconds=30)], "interval[]", None],
        [[{"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}], "jsonb[]", None],
        [[b"\x00\x01\x02\x03\x02\x01\x00"], "bytea[]", None],
        [[Decimal("1.1"), None, Decimal("3.3")], "numeric[]", None],
        [[UUID("911460f2-1f43-fea2-3e2c-e01fd5b5069d")], "uuid[]", None],
        [
            [
                "Hello!",
                "World!",
                "abcdefghijklmnopqrstuvwxyz",
                "",
                "A bunch of random characters:",
                " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t",
                "\n",
                "\r",
                "\t",
                "\b",
                None,
            ],
            "varchar[]",
            None,
        ],
        [[], "varchar[]", None],
        [Time(4, 5, 6), "time", None],
        [Date(2001, 2, 3), "date", None],
        ["infinity", "date", None],
        [Datetime(2001, 2, 3, 4, 5, 6), "timestamp", None],
        [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc), "timestamptz", None],
        [True, "bool", None],
        [Decimal("1.1"), "numeric", None],
        [1.756e-12, "float8", None],
        [float("inf"), "float8", None],
        ["hello world", "unknown", None],
        ["hello \u0173 world", "varchar", None],
        [50000000000000, "int8", None],
        [b"\x00\x01\x02\x03\x02\x01\x00", "bytea", None],
        [bytearray(b"\x00\x01\x02\x03\x02\x01\x00"), "bytea", None],
        [UUID("911460f2-1f43-fea2-3e2c-e01fd5b5069d"), "uuid", None],
        [IPv4Network("192.168.0.0/28"), "inet", None],
        [IPv4Address("192.168.0.1"), "inet", None],
    ],
)
def test_roundtrip_cast(con, pg_version, test_input, typ, req_ver):
    if req_ver is None or pg_version >= req_ver:
        retval = con.run(f"SELECT CAST(:v AS {typ})", v=test_input)
        assert retval[0][0] == test_input


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("SELECT CAST('{a,b,c}' AS TEXT[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS CHAR[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS VARCHAR[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS CSTRING[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS NAME[])", ["a", "b", "c"]),
        ("SELECT CAST('{}' AS text[])", []),
        ('SELECT CAST(\'{NULL,"NULL",NULL,""}\' AS text[])', [None, "NULL", None, ""]),
    ],
)
def test_array_in(con, test_input, expected):
    result = con.run(test_input)
    assert result[0][0] == expected


def test_macaddr(con):
    retval = con.run("SELECT macaddr '08002b:010203'")
    assert retval[0][0] == "08:00:2b:01:02:03"


def test_tsvector_roundtrip(con):
    retval = con.run(
        "SELECT cast(:v as tsvector)", v="a fat cat sat on a mat and ate a fat rat"
    )
    assert retval[0][0] == "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"


def test_json_access_object(con):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    retval = con.run("SELECT cast(:val as json) -> :name", val=dumps(val), name="name")
    assert retval[0][0] == "Apollo 11 Cave"


def test_jsonb_access_object(con):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    retval = con.run("SELECT cast(:val as jsonb) -> :name", val=dumps(val), name="name")
    assert retval[0][0] == "Apollo 11 Cave"


def test_json_access_array(con):
    val = [-1, -2, -3, -4, -5]
    retval = con.run(
        "SELECT cast(:v1 as json) -> cast(:v2 as int)", v1=dumps(val), v2=2
    )
    assert retval[0][0] == -3


def test_jsonb_access_array(con):
    val = [-1, -2, -3, -4, -5]
    retval = con.run(
        "SELECT cast(:v1 as jsonb) -> cast(:v2 as int)", v1=dumps(val), v2=2
    )
    assert retval[0][0] == -3


def test_jsonb_access_path(con):
    j = {"a": [1, 2, 3], "b": [4, 5, 6]}

    path = ["a", "2"]

    retval = con.run("SELECT cast(:v1 as jsonb) #>> :v2", v1=dumps(j), v2=path)
    assert retval[0][0] == str(j[path[0]][int(path[1])])


@pytest.fixture
def duple_type(con):
    con.run("CREATE TYPE duple AS (a int, b int);")
    yield
    con.run("DROP TYPE IF EXISTS duple;")


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ((1, 3), "(1,3)"),
        ((1, None), "(1,)"),
    ],
)
def test_composite_type(con, duple_type, test_input, expected):
    retval = con.run("SELECT CAST(:v AS duple)", v=test_input)
    assert retval[0][0] == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ([(1, 3)], '{"(1,3)"}'),
    ],
)
def test_composite_type_array(con, duple_type, test_input, expected):
    retval = con.run("SELECT CAST(:v AS duple[])", v=test_input)
    assert retval[0][0] == expected
