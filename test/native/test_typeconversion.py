import os
import time
from collections import OrderedDict
from datetime import (
    date as Date,
    datetime as Datetime,
    time as Time,
    timedelta as Timedelta,
    timezone as Timezone)
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address, IPv4Network
from json import dumps
from uuid import UUID

import pg8000
from pg8000.converters import (
    BIGINT, BIGINT_ARRAY, BOOLEAN, CIDR_ARRAY, DATE, FLOAT_ARRAY, INET,
    INTEGER_ARRAY, JSON, JSONB, JSONB_ARRAY, JSON_ARRAY, MONEY, MONEY_ARRAY,
    NUMERIC, NUMERIC_ARRAY, POINT, SMALLINT_ARRAY, TIME, TIMESTAMP,
    TIMESTAMPTZ, TIMESTAMPTZ_ARRAY, TIMESTAMP_ARRAY, UUID_ARRAY, UUID_TYPE,
    XID, pginterval_in, timedelta_in
)

import pytest

import pytz


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
        int2: 'int2',
        int4: 'int4',
        int8: 'int8',
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
        (+9223372036854775807, int8)]

    for value, typoid in test_values:
        retval = con.run("SELECT cast(:v as " + MAP[typoid] + ")", v=value)
        assert retval[0][0] == value
        column_type_oid = con.columns[0]['type_oid']
        assert column_type_oid == typoid


def test_bytearray_subclass_round_trip(con):
    class BClass(bytearray):
        pass
    binary = b'\x00\x01\x02\x03\x02\x01\x00'
    retval = con.run("SELECT cast(:v as bytea)", v=BClass(binary))
    assert retval[0][0] == binary


def test_timestamp_roundtrip(is_java, con):
    v = Datetime(2001, 2, 3, 4, 5, 6, 170000)
    retval = con.run("SELECT cast(:v as timestamp)", v=v)
    assert retval[0][0] == v

    # Test that time zone doesn't affect it
    # Jython 2.5.3 doesn't have a time.tzset() so skip
    if not is_java:
        orig_tz = os.environ.get('TZ')
        os.environ['TZ'] = "America/Edmonton"
        time.tzset()

        retval = con.run("SELECT cast(:v as timestamp)", v=v)
        assert retval[0][0] == v

        if orig_tz is None:
            del os.environ['TZ']
        else:
            os.environ['TZ'] = orig_tz
        time.tzset()


def test_interval_repr():
    v = pg8000.PGInterval(microseconds=123456789, days=2, months=24)
    assert repr(v) == '<PGInterval 24 months 2 days 123456789 microseconds>'


def test_interval_in_1_year():
    assert pginterval_in('1 year') == pg8000.PGInterval(years=1)


def test_timedelta_in_2_months():
    assert timedelta_in('2 hours')


def test_interval_roundtrip(con):
    con.register_in_adapter(1186, pginterval_in)
    v = pg8000.PGInterval(microseconds=123456789, days=2, months=24)
    retval = con.run("SELECT cast(:v as interval)", v=v)
    assert retval[0][0] == v


def test_enum_str_round_trip(con):
    try:
        con.run("create type lepton as enum ('electron', 'muon', 'tau')")

        v = 'muon'
        retval = con.run("SELECT cast(:v as lepton) as f1", v=v)
        assert retval[0][0] == v
        con.run("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        con.run(
            "INSERT INTO testenum VALUES (cast(:v as lepton))", v='electron')
    finally:
        con.run("drop table testenum")
        con.run("drop type lepton")


def test_enum_custom_round_trip(con):
    class Lepton():
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
        lepton_oid = con.run(
            "select oid from pg_type where typname = 'lepton'")[0][0]
        con.register_out_adapter(Lepton, lepton_oid, lepton_out)

        v = Lepton('muon', '2')
        retval = con.run("SELECT :v", v=v)
        assert retval[0][0] == v.value
    finally:
        con.run("drop type lepton")


def test_enum_py_round_trip(con):
    class Lepton(Enum):
        electron = '1'
        muon = '2'
        tau = '3'

    try:
        con.run("create type lepton as enum ('1', '2', '3')")

        v = Lepton.muon
        retval = con.run("SELECT cast(:v as lepton) as f1", v=v)
        assert retval[0][0] == v.value

        con.run("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        con.run(
            "INSERT INTO testenum VALUES (cast(:v as lepton))",
            v=Lepton.electron)
    finally:
        con.run("drop table testenum")
        con.run("drop type lepton")


def test_xml_roundtrip(con):
    v = '<genome>gatccgagtac</genome>'
    retval = con.run("select xmlparse(content :v) as f1", v=v)
    assert retval[0][0] == v


def test_int2vector_in(con):
    retval = con.run("select cast('1 2' as int2vector) as f1")
    assert retval[0][0] == [1, 2]

    # Should complete without an exception
    con.run("select indkey from pg_index")


def test_timestamp_tz_out(con):
    retval = con.run(
        "SELECT '2001-02-03 04:05:06.17 America/Edmonton'"
        "::timestamp with time zone")
    dt = retval[0][0]
    assert dt.tzinfo is not None, "no tzinfo returned"
    assert dt.astimezone(Timezone.utc) == Datetime(
        2001, 2, 3, 11, 5, 6, 170000, Timezone.utc), \
        "retrieved value match failed"


def test_timestamp_tz_roundtrip(is_java, con):
    if not is_java:
        mst = pytz.timezone("America/Edmonton")
        v1 = mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000))
        retval = con.run("SELECT cast(:v as timestamptz)", v=v1)
        v2 = retval[0][0]
        assert v2.tzinfo is not None
        assert v1 == v2


def test_timestamp_mismatch(is_java, con):
    if not is_java:
        mst = pytz.timezone("America/Edmonton")
        con.run("SET SESSION TIME ZONE 'America/Edmonton'")
        try:
            con.run(
                "CREATE TEMPORARY TABLE TestTz "
                "(f1 timestamp with time zone, "
                "f2 timestamp without time zone)")
            con.run(
                "INSERT INTO TestTz (f1, f2) VALUES (:v1, :v2)",
                # insert timestamp into timestamptz field (v1)
                v1=Datetime(2001, 2, 3, 4, 5, 6, 170000),
                # insert timestamptz into timestamp field (v2)
                v2=mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000)))
            retval = con.run("SELECT f1, f2 FROM TestTz")

            # when inserting a timestamp into a timestamptz field,
            # postgresql assumes that it is in local time. So the value
            # that comes out will be the server's local time interpretation
            # of v1. We've set the server's TZ to MST, the time should
            # be...
            f1 = retval[0][0]
            assert f1 == Datetime(2001, 2, 3, 11, 5, 6, 170000, Timezone.utc)

            # inserting the timestamptz into a timestamp field, pg8000
            # converts the value into UTC, and then the PG server converts
            # it into local time for insertion into the field. When we
            # query for it, we get the same time back, like the tz was
            # dropped.
            f2 = retval[0][1]
            assert f2 == Datetime(2001, 2, 3, 4, 5, 6, 170000)
        finally:
            con.run("SET SESSION TIME ZONE DEFAULT")


def test_name_out(con):
    # select a field that is of "name" type:
    con.run("SELECT usename FROM pg_user")
    # It is sufficient that no errors were encountered.


def test_oid_out(con):
    con.run("SELECT oid FROM pg_type")
    # It is sufficient that no errors were encountered.


def test_boolean_in(con):
    retval = con.run("SELECT cast('t' as bool)")
    assert retval[0][0]


def test_numeric_out(con):
    for num in ('5000', '50.34'):
        retval = con.run("SELECT " + num + "::numeric")
        assert str(retval[0][0]) == num


def test_int2_out(con):
    retval = con.run("SELECT 5000::smallint")
    assert retval[0][0] == 5000


def test_int4_out(con):
    retval = con.run("SELECT 5000::integer")
    assert retval[0][0] == 5000


def test_int8_out(con):
    retval = con.run("SELECT 50000000000000::bigint")
    assert retval[0][0] == 50000000000000


def test_float4_out(con):
    retval = con.run("SELECT 1.1::real")
    assert retval[0][0] == 1.1


def test_float8_out(con):
    retval = con.run("SELECT 1.1::double precision")
    assert retval[0][0] == 1.1000000000000001


def test_varchar_out(con):
    retval = con.run("SELECT 'hello'::varchar(20)")
    assert retval[0][0] == "hello"


def test_char_out(con):
    retval = con.run("SELECT 'hello'::char(20)")
    assert retval[0][0] == "hello               "


def test_text_out(con):
    retval = con.run("SELECT 'hello'::text")
    assert retval[0][0] == "hello"


def test_interval_in(con):
    con.register_in_adapter(1186, pg8000.converters.pginterval_in)
    retval = con.run(
        "SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'::interval")
    expected_value = pg8000.PGInterval(
        microseconds=(12 * 60 * 60 * 1000 * 1000) +
        (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000), days=16, months=1)
    assert retval[0][0] == expected_value


def test_interval_in_30_seconds(con):
    retval = con.run("select interval '30 seconds'")
    assert retval[0][0] == Timedelta(seconds=30)


def test_interval_in_12_days_30_seconds(con):
    retval = con.run("select interval '12 days 30 seconds'")
    assert retval[0][0] == Timedelta(days=12, seconds=30)


def test_timestamp_out(con):
    retval = con.run("SELECT '2001-02-03 04:05:06.17'::timestamp")
    assert retval[0][0] == Datetime(2001, 2, 3, 4, 5, 6, 170000)


def test_int4_array_out(con):
    retval = con.run(
        "SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3")
    f1, f2, f3 = retval[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int2_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::INT2[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT2[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3")
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int8_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::INT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3")
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_bool_array_out(con):
    res = con.run(
        "SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, "
        "'{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, "
        "'{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
        "::BOOL[][][] AS f3")
    f1, f2, f3 = res[0]
    assert f1 == [True, False, False, True]
    assert f2 == [[True, False, True], [False, True, False]]
    assert f3 == [
        [[True, False], [False, True]], [[None, True], [False, False]]]


def test_float4_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::FLOAT4[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3")
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_float8_array_out(con):
    res = con.run(
        "SELECT '{1,2,3,4}'::FLOAT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3")
    f1, f2, f3 = res[0]
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


@pytest.mark.parametrize(
    "test_input,oid",
    [
        [[True, False, None], None],  # bool[]
        [[IPv4Address('192.168.0.1')], None],  # inet[]
        [[Date(2021, 3, 1)], None],  # date[]
        [[Datetime(2001, 2, 3, 4, 5, 6)], None],  # timestamp[]
        [[Datetime(2001, 2, 3, 4, 5, 6)], TIMESTAMP_ARRAY],  # timestamp[]
        [  # timestamptz
            [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc)], None
        ],
        [  # timestamptz[]
            [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc)], TIMESTAMPTZ_ARRAY
        ],
        [
            {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003},
            # json
            JSON
        ],
        [  # jsonb
            {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003},
            JSONB
        ],
        [  # jsonb
            {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003},
            None
        ],
        [[b'\x00\x01\x02\x03\x02\x01\x00'], None],  # bytea[]
        [[IPv4Network('192.168.0.0/28')], CIDR_ARRAY],  # cidr[]
        [[1, 2, 3], SMALLINT_ARRAY],  # int2[]
        [[[1, 2], [3, 4]], SMALLINT_ARRAY],  # int2[] multidimensional
        [[1, None, 3], INTEGER_ARRAY],  # int4[] with None
        [[7000000000, 2, 3], BIGINT_ARRAY],  # int8[]
        [[1.1, 2.2, 3.3], FLOAT_ARRAY],  # float8[]
        [[Decimal("1.1"), None, Decimal("3.3")], None],  # numeric[]
        [[Decimal("1.1"), None, Decimal("3.3")], NUMERIC_ARRAY],  # numeric[]
        [["1.10", None, "3.30"], MONEY_ARRAY],  # money[]
        [[UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')], None],  # uuid[]
        [[UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')], UUID_ARRAY],  # uuid[]
        [
            [
                "Hello!", "World!", "abcdefghijklmnopqrstuvwxyz", "",
                "A bunch of random characters:",
                " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t", None
            ],
            None
        ],
        [  # json[]
            [{'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}],
            JSON_ARRAY
        ],
        [  # jsonb[]
            [{'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}],
            JSONB_ARRAY
        ],
        [Timedelta(seconds=30), None],  # interval
        [Time(4, 5, 6), None],  # time
        [Time(4, 5, 6), TIME],  # time
        [Date(2001, 2, 3), None],  # date
        [Date(2001, 2, 3), DATE],  # date
        [Datetime(2001, 2, 3, 4, 5, 6), None],  # timestamp
        [Datetime(2001, 2, 3, 4, 5, 6), TIMESTAMP],  # timestamp
        [Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc), None],  # timestamptz
        [  # timestamptz
            Datetime(2001, 2, 3, 4, 5, 6, 0, Timezone.utc), TIMESTAMPTZ
        ],
        [True, None],  # bool
        [True, BOOLEAN],  # bool
        [None, BOOLEAN],  # null (needs a type on PostgreSQL 9.6)
        [Decimal("1.1"), None],  # numeric
        [Decimal("1.1"), NUMERIC],  # numeric
        ["1.10", MONEY],  # money
        [1.756e-12, None],  # float8
        [float('inf'), None],  # float8
        ["hello world", None],  # unknown
        ["hello \u0173 world", None],  # unknown
        [50000000000000, None],  # int8
        [50000000000000, BIGINT],  # int8
        [b"\x00\x01\x02\x03\x02\x01\x00", None],  # bytea
        [bytearray(b'\x00\x01\x02\x03\x02\x01\x00'), None],  # bytea
        [UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d'), None],  # uuid
        [UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d'), UUID_TYPE],  # uuid
        [IPv4Network('192.168.0.0/28'), None],  # inet
        [IPv4Network('192.168.0.0/28'), INET],  # inet
        [IPv4Address('192.168.0.1'), None],  # inet
        [IPv4Address('192.168.0.1'), INET],  # inet
        [86722, XID],  # xid
        ['infinity', TIMESTAMP],  # timestamp
        ['(2.3,1)', POINT],  # point
        [  # json
            {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003},
            JSON
        ],
        [  # jsonb
            {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003},
            JSONB
        ],
    ]
)
def test_roundtrip(con, pg_version, test_input, oid):
    if oid is None and pg_version < 10:
        return

    types = None if oid is None else {'v': oid}

    retval = con.run("SELECT :v", v=test_input, types=types)
    assert retval[0][0] == test_input

    if types is not None:
        column_type_oid = con.columns[0]['type_oid']
        assert column_type_oid == types['v']


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("SELECT CAST('{a,b,c}' AS TEXT[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS CHAR[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS VARCHAR[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS CSTRING[])", ["a", "b", "c"]),
        ("SELECT CAST('{a,b,c}' AS NAME[])", ["a", "b", "c"]),
        ("SELECT CAST('{}' AS text[])", []),
        (
            "SELECT CAST('{NULL,\"NULL\",NULL,\"\"}' AS text[])",
            [None, 'NULL', None, ""]
        )
    ]
)
def test_array_in(con, test_input, expected):
    result = con.run(test_input)
    assert result[0][0] == expected


def test_numeric_array_out(con):
    res = con.run("SELECT '{1.1,2.2,3.3}'::numeric[] AS f1")
    assert res[0][0] == [Decimal("1.1"), Decimal("2.2"), Decimal("3.3")]


def test_array_string_escape():
    v = "\""
    res = pg8000.converters.array_string_escape(v)
    assert res == '"\\""'


def test_empty_array(con):
    v = []
    retval = con.run("SELECT cast(:v as varchar[])", v=v)
    assert retval[0][0] == v


def test_macaddr(con):
    retval = con.run("SELECT macaddr '08002b:010203'")
    assert retval[0][0] == "08:00:2b:01:02:03"


def test_tsvector_roundtrip(con):
    retval = con.run(
        "SELECT cast(:v as tsvector)",
        v='a fat cat sat on a mat and ate a fat rat')
    assert retval[0][0] == "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"


def test_hstore_roundtrip(con):
    val = '"a"=>"1"'
    retval = con.run("SELECT cast(:val as hstore)", val=val)
    assert retval[0][0] == val


def test_json_access_object(con):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    retval = con.run(
        "SELECT cast(:val as json) -> :name", val=dumps(val), name='name')
    assert retval[0][0] == 'Apollo 11 Cave'


def test_jsonb_access_object(con):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    retval = con.run(
        "SELECT cast(:val as jsonb) -> :name", val=dumps(val), name='name')
    assert retval[0][0] == 'Apollo 11 Cave'


def test_json_access_array(con):
    val = [-1, -2, -3, -4, -5]
    retval = con.run(
        "SELECT cast(:v1 as json) -> cast(:v2 as int)",
        v1=dumps(val), v2=2)
    assert retval[0][0] == -3


def test_jsonb_access_array(con):
    val = [-1, -2, -3, -4, -5]
    retval = con.run(
        "SELECT cast(:v1 as jsonb) -> cast(:v2 as int)", v1=dumps(val), v2=2)
    assert retval[0][0] == -3


def test_jsonb_access_path(con):
    j = {
        "a": [1, 2, 3],
        "b": [4, 5, 6]}

    path = ['a', '2']

    retval = con.run(
        "SELECT cast(:v1 as jsonb) #>> :v2", v1=dumps(j), v2=path)
    assert retval[0][0] == str(j[path[0]][int(path[1])])


def test_time_in():
    actual = pg8000.converters.time_in("12:57:18.000396")
    assert actual == Time(12, 57, 18, 396)
