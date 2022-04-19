import decimal
import ipaddress
import os
import time
import uuid
from collections import OrderedDict
from datetime import (
    date as Date,
    datetime as Datetime,
    time as Time,
    timedelta as Timedelta,
    timezone as Timezone,
)
from enum import Enum
from json import dumps

import pytest

import pytz

import pg8000.converters
from pg8000 import (
    Binary,
    INTERVAL,
    PGInterval,
    pginterval_in,
    pginterval_out,
    timedelta_in,
)


# Type conversion tests


def test_time_roundtrip(con):
    retval = con.run("SELECT cast(:t as time) as f1", t=Time(4, 5, 6))
    assert retval[0][0] == Time(4, 5, 6)


def test_date_roundtrip(con):
    v = Date(2001, 2, 3)
    retval = con.run("SELECT cast(:d as date) as f1", d=v)
    assert retval[0][0] == v


def test_bool_roundtrip(con):
    retval = con.run("SELECT cast(:b as bool) as f1", b=True)
    assert retval[0][0] is True


def test_null_roundtrip(con):
    retval = con.run("SELECT :v", v=None)
    assert retval[0][0] is None


def test_decimal_roundtrip(cursor):
    values = ("1.1", "-1.1", "10000", "20000", "-1000000000.123456789", "1.0", "12.44")
    for v in values:
        cursor.execute("SELECT CAST(%s AS NUMERIC)", (decimal.Decimal(v),))
        retval = cursor.fetchall()
        assert str(retval[0][0]) == v


def test_float_roundtrip(con):
    val = 1.756e-12
    retval = con.run("SELECT cast(:v as double precision)", v=val)
    assert retval[0][0] == val


def test_float_plus_infinity_roundtrip(con):
    v = float("inf")
    retval = con.run("SELECT cast(:v as double precision)", v=v)
    assert retval[0][0] == v


def test_str_roundtrip(cursor):
    v = "hello world"
    cursor.execute("create temporary table test_str (f character varying(255))")
    cursor.execute("INSERT INTO test_str VALUES (%s)", (v,))
    retval = tuple(cursor.execute("SELECT * from test_str"))
    assert retval[0][0] == v


def test_str_then_int(cursor):
    v1 = "hello world"
    retval = tuple(cursor.execute("SELECT cast(%s as varchar) as f1", (v1,)))
    assert retval[0][0] == v1

    v2 = 1
    retval = tuple(cursor.execute("SELECT cast(%s as varchar) as f1", (v2,)))
    assert retval[0][0] == str(v2)


def test_unicode_roundtrip(cursor):
    v = "hello \u0173 world"
    retval = tuple(cursor.execute("SELECT cast(%s as varchar) as f1", (v,)))
    assert retval[0][0] == v


def test_long_roundtrip(con):
    v = 50000000000000
    retval = con.run("SELECT cast(:v as bigint)", v=v)
    assert retval[0][0] == v


def test_int_execute_many_select(cursor):
    tuple(cursor.executemany("SELECT CAST(%s AS INTEGER)", ((1,), (40000,))))


def test_int_execute_many_insert(cursor):
    v = ([None], [4])
    cursor.execute("create temporary table test_int (f integer)")
    cursor.executemany("INSERT INTO test_int VALUES (%s)", v)
    retval = tuple(cursor.execute("SELECT * from test_int"))
    assert retval == v


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
        column_name, column_typeoid = con.description[0][0:2]
        assert column_typeoid == typoid


def test_bytea_roundtrip(con):
    retval = con.run(
        "SELECT cast(:v as bytea)", v=Binary(b"\x00\x01\x02\x03\x02\x01\x00")
    )
    assert retval[0][0] == b"\x00\x01\x02\x03\x02\x01\x00"


def test_bytearray_round_trip(con):
    binary = b"\x00\x01\x02\x03\x02\x01\x00"
    retval = con.run("SELECT cast(:v as bytea)", v=bytearray(binary))
    assert retval[0][0] == binary


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


def test_interval_repr():
    v = PGInterval(microseconds=123456789, days=2, months=24)
    assert repr(v) == "<PGInterval 24 months 2 days 123456789 microseconds>"


def test_interval_in_1_year():
    assert pginterval_in("1 year") == PGInterval(years=1)


def test_timedelta_in_2_months():
    assert timedelta_in("2 hours")


def test_interval_roundtrip(con):
    con.register_in_adapter(INTERVAL, pginterval_in)
    con.register_out_adapter(PGInterval, pginterval_out)
    v = PGInterval(microseconds=123456789, days=2, months=24)
    retval = con.run("SELECT cast(:v as interval)", v=v)
    assert retval[0][0] == v


def test_timedelta_roundtrip(con):
    v = Timedelta(seconds=30)
    retval = con.run("SELECT cast(:v as interval)", v=v)
    assert retval[0][0] == v


def test_enum_str_round_trip(cursor):
    try:
        cursor.execute("create type lepton as enum ('electron', 'muon', 'tau')")

        v = "muon"
        cursor.execute("SELECT cast(%s as lepton) as f1", (v,))
        retval = cursor.fetchall()
        assert retval[0][0] == v
        cursor.execute("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        cursor.execute(
            "INSERT INTO testenum VALUES (cast(%s as lepton))", ("electron",)
        )
    finally:
        cursor.execute("drop table testenum")
        cursor.execute("drop type lepton")


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


def test_enum_py_round_trip(cursor):
    class Lepton(Enum):
        electron = "1"
        muon = "2"
        tau = "3"

    try:
        cursor.execute("create type lepton as enum ('1', '2', '3')")

        v = Lepton.muon
        retval = tuple(cursor.execute("SELECT cast(%s as lepton) as f1", (v,)))
        assert retval[0][0] == v.value

        cursor.execute("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        cursor.execute(
            "INSERT INTO testenum VALUES (cast(%s as lepton))", (Lepton.electron,)
        )
    finally:
        cursor.execute("drop table testenum")
        cursor.execute("drop type lepton")


def test_xml_roundtrip(cursor):
    v = "<genome>gatccgagtac</genome>"
    retval = tuple(cursor.execute("select xmlparse(content %s) as f1", (v,)))
    assert retval[0][0] == v


def test_uuid_roundtrip(con):
    v = uuid.UUID("911460f2-1f43-fea2-3e2c-e01fd5b5069d")
    retval = con.run("select cast(:v as uuid)", v=v)
    assert retval[0][0] == v


def test_inet_roundtrip_network(con):
    v = ipaddress.ip_network("192.168.0.0/28")
    retval = con.run("select cast(:v as inet)", v=v)
    assert retval[0][0] == v


def test_inet_roundtrip_address(con):
    v = ipaddress.ip_address("192.168.0.1")
    retval = con.run("select cast(:v as inet)", v=v)
    assert retval[0][0] == v


def test_xid_roundtrip(cursor):
    v = 86722
    cursor.execute("select cast(cast(%s as varchar) as xid) as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v

    # Should complete without an exception
    cursor.execute("select * from pg_locks where transactionid = %s", (97712,))
    retval = cursor.fetchall()


def test_int2vector_in(cursor):
    retval = tuple(cursor.execute("select cast('1 2' as int2vector) as f1"))
    assert retval[0][0] == [1, 2]

    # Should complete without an exception
    tuple(cursor.execute("select indkey from pg_index"))


def test_timestamp_tz_out(cursor):
    cursor.execute(
        "SELECT '2001-02-03 04:05:06.17 America/Edmonton'" "::timestamp with time zone"
    )
    retval = cursor.fetchall()
    dt = retval[0][0]
    assert dt.tzinfo is not None, "no tzinfo returned"
    assert dt.astimezone(Timezone.utc) == Datetime(
        2001, 2, 3, 11, 5, 6, 170000, Timezone.utc
    ), "retrieved value match failed"


def test_timestamp_tz_roundtrip(con):
    mst = pytz.timezone("America/Edmonton")
    v1 = mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000))
    retval = con.run("SELECT cast(:v as timestamptz)", v=v1)
    v2 = retval[0][0]
    assert v2.tzinfo is not None
    assert v1 == v2


def test_timestamp_mismatch(cursor):
    mst = pytz.timezone("America/Edmonton")
    cursor.execute("SET SESSION TIME ZONE 'America/Edmonton'")
    try:
        cursor.execute(
            "CREATE TEMPORARY TABLE TestTz "
            "(f1 timestamp with time zone, "
            "f2 timestamp without time zone)"
        )
        cursor.execute(
            "INSERT INTO TestTz (f1, f2) VALUES (%s, %s)",
            (
                # insert timestamp into timestamptz field (v1)
                Datetime(2001, 2, 3, 4, 5, 6, 170000),
                # insert timestamptz into timestamp field (v2)
                mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000)),
            ),
        )
        cursor.execute("SELECT f1, f2 FROM TestTz")
        retval = cursor.fetchall()

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
        assert f2 == Datetime(2001, 2, 3, 11, 5, 6, 170000)
    finally:
        cursor.execute("SET SESSION TIME ZONE DEFAULT")


def test_name_out(cursor):
    # select a field that is of "name" type:
    tuple(cursor.execute("SELECT usename FROM pg_user"))
    # It is sufficient that no errors were encountered.


def test_oid_out(cursor):
    tuple(cursor.execute("SELECT oid FROM pg_type"))
    # It is sufficient that no errors were encountered.


def test_boolean_in(cursor):
    retval = tuple(cursor.execute("SELECT cast('t' as bool)"))
    assert retval[0][0]


def test_numeric_out(cursor):
    for num in ("5000", "50.34"):
        retval = tuple(cursor.execute("SELECT " + num + "::numeric"))
        assert str(retval[0][0]) == num


def test_int2_out(cursor):
    retval = tuple(cursor.execute("SELECT 5000::smallint"))
    assert retval[0][0] == 5000


def test_int4_out(cursor):
    retval = tuple(cursor.execute("SELECT 5000::integer"))
    assert retval[0][0] == 5000


def test_int8_out(cursor):
    retval = tuple(cursor.execute("SELECT 50000000000000::bigint"))
    assert retval[0][0] == 50000000000000


def test_float4_out(cursor):
    retval = tuple(cursor.execute("SELECT 1.1::real"))
    assert retval[0][0] == 1.1


def test_float8_out(cursor):
    retval = tuple(cursor.execute("SELECT 1.1::double precision"))
    assert retval[0][0] == 1.1000000000000001


def test_varchar_out(cursor):
    retval = tuple(cursor.execute("SELECT 'hello'::varchar(20)"))
    assert retval[0][0] == "hello"


def test_char_out(cursor):
    retval = tuple(cursor.execute("SELECT 'hello'::char(20)"))
    assert retval[0][0] == "hello               "


def test_text_out(cursor):
    retval = tuple(cursor.execute("SELECT 'hello'::text"))
    assert retval[0][0] == "hello"


def test_interval_in(con):
    con.register_in_adapter(INTERVAL, pginterval_in)
    retval = con.run(
        "SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'::interval"
    )
    expected_value = PGInterval(
        microseconds=(12 * 60 * 60 * 1000 * 1000)
        + (32 * 60 * 1000 * 1000)
        + (64 * 1000 * 1000),
        days=16,
        months=1,
    )
    assert retval[0][0] == expected_value


def test_interval_in_30_seconds(con):
    retval = con.run("select interval '30 seconds'")
    assert retval[0][0] == Timedelta(seconds=30)


def test_interval_in_12_days_30_seconds(con):
    retval = con.run("select interval '12 days 30 seconds'")
    assert retval[0][0] == Timedelta(days=12, seconds=30)


def test_timestamp_out(cursor):
    cursor.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
    retval = cursor.fetchall()
    assert retval[0][0] == Datetime(2001, 2, 3, 4, 5, 6, 170000)


def test_int4_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int2_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT2[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT2[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int8_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_bool_array_out(cursor):
    cursor.execute(
        "SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, "
        "'{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, "
        "'{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
        "::BOOL[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [True, False, False, True]
    assert f2 == [[True, False, True], [False, True, False]]
    assert f3 == [[[True, False], [False, True]], [[None, True], [False, False]]]


def test_float4_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::FLOAT4[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_float8_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::FLOAT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3"
    )
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int_array_roundtrip_small(con):
    """send small int array, should be sent as INT2[]"""
    retval = con.run("SELECT cast(:v as int2[])", v=[1, 2, 3])
    assert retval[0][0], [1, 2, 3]
    column_name, column_typeoid = con.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"


def test_int_array_roundtrip_multi(con):
    """test multi-dimensional array, should be sent as INT2[]"""
    retval = con.run("SELECT cast(:v as int2[])", v=[[1, 2], [3, 4]])
    assert retval[0][0] == [[1, 2], [3, 4]]

    column_name, column_typeoid = con.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"


def test_int4_array_roundtrip(con):
    """a larger value should kick it up to INT4[]..."""
    retval = con.run("SELECT cast(:v as int4[])", v=[70000, 2, 3])
    assert retval[0][0] == [70000, 2, 3]
    column_name, column_typeoid = con.description[0][0:2]
    assert column_typeoid == 1007, "type should be INT4[]"


def test_int8_array_roundtrip(con):
    """a much larger value should kick it up to INT8[]..."""
    retval = con.run("SELECT cast(:v as int8[])", v=[7000000000, 2, 3])
    assert retval[0][0] == [7000000000, 2, 3], "retrieved value match failed"
    column_name, column_typeoid = con.description[0][0:2]
    assert column_typeoid == 1016, "type should be INT8[]"


def test_int_array_with_null_roundtrip(con):
    retval = con.run("SELECT cast(:v as int[])", v=[1, None, 3])
    assert retval[0][0] == [1, None, 3]


def test_float_array_roundtrip(con):
    retval = con.run("SELECT cast(:v as double precision[])", v=[1.1, 2.2, 3.3])
    assert retval[0][0] == [1.1, 2.2, 3.3]


def test_bool_array_roundtrip(con):
    retval = con.run("SELECT cast(:v as bool[])", v=[True, False, None])
    assert retval[0][0] == [True, False, None]


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("SELECT '{a,b,c}'::TEXT[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::CHAR[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::VARCHAR[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::CSTRING[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::NAME[] AS f1", ["a", "b", "c"]),
        ("SELECT '{}'::text[];", []),
        ('SELECT \'{NULL,"NULL",NULL,""}\'::text[];', [None, "NULL", None, ""]),
    ],
)
def test_string_array_out(con, test_input, expected):
    result = con.run(test_input)
    assert result[0][0] == expected


def test_numeric_array_out(cursor):
    cursor.execute("SELECT '{1.1,2.2,3.3}'::numeric[] AS f1")
    assert cursor.fetchone()[0] == [
        decimal.Decimal("1.1"),
        decimal.Decimal("2.2"),
        decimal.Decimal("3.3"),
    ]


def test_numeric_array_roundtrip(con):
    v = [decimal.Decimal("1.1"), None, decimal.Decimal("3.3")]
    retval = con.run("SELECT cast(:v as numeric[])", v=v)
    assert retval[0][0] == v


def test_string_array_roundtrip(con):
    v = [
        "Hello!",
        "World!",
        "abcdefghijklmnopqrstuvwxyz",
        "",
        "A bunch of random characters:",
        " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t",
        None,
    ]
    retval = con.run("SELECT cast(:v as varchar[])", v=v)
    assert retval[0][0] == v


def test_array_string_escape():
    v = '"'
    res = pg8000.converters.array_string_escape(v)
    assert res == '"\\""'


def test_empty_array(con):
    v = []
    retval = con.run("SELECT cast(:v as varchar[])", v=v)
    assert retval[0][0] == v


def test_macaddr(cursor):
    retval = tuple(cursor.execute("SELECT macaddr '08002b:010203'"))
    assert retval[0][0] == "08:00:2b:01:02:03"


def test_tsvector_roundtrip(cursor):
    cursor.execute(
        "SELECT cast(%s as tsvector)", ("a fat cat sat on a mat and ate a fat rat",)
    )
    retval = cursor.fetchall()
    assert retval[0][0] == "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"


def test_hstore_roundtrip(cursor):
    val = '"a"=>"1"'
    retval = tuple(cursor.execute("SELECT cast(%s as hstore)", (val,)))
    assert retval[0][0] == val


def test_json_roundtrip(con):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    retval = con.run("SELECT cast(:v as jsonb)", v=dumps(val))
    assert retval[0][0] == val


def test_jsonb_roundtrip(cursor):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    cursor.execute("SELECT cast(%s as jsonb)", (dumps(val),))
    retval = cursor.fetchall()
    assert retval[0][0] == val


def test_json_access_object(cursor):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    cursor.execute("SELECT cast(%s as json) -> %s", (dumps(val), "name"))
    retval = cursor.fetchall()
    assert retval[0][0] == "Apollo 11 Cave"


def test_jsonb_access_object(cursor):
    val = {"name": "Apollo 11 Cave", "zebra": True, "age": 26.003}
    cursor.execute("SELECT cast(%s as jsonb) -> %s", (dumps(val), "name"))
    retval = cursor.fetchall()
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


def test_infinity_timestamp_roundtrip(cursor):
    v = "infinity"
    retval = tuple(cursor.execute("SELECT cast(%s as timestamp) as f1", (v,)))
    assert retval[0][0] == v


def test_point_roundtrip(cursor):
    v = "(2.3,1)"
    retval = tuple(cursor.execute("SELECT cast(%s as point) as f1", (v,)))
    assert retval[0][0] == v


def test_time_in():
    actual = pg8000.converters.time_in("12:57:18.000396")
    assert actual == Time(12, 57, 18, 396)
