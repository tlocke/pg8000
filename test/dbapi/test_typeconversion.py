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
    timezone as Timezone)
from enum import Enum
from json import dumps

import pg8000
from pg8000 import converters

import pytest

import pytz


# Type conversion tests

def test_time_roundtrip(cursor):
    t = Time(4, 5, 6)
    cursor.execute("SELECT cast(%s as time) as f1", (t,))
    assert cursor.fetchall()[0][0] == t


def test_date_roundtrip(cursor):
    v = Date(2001, 2, 3)
    cursor.execute("SELECT cast(%s as date) as f1", (v,))
    assert cursor.fetchall()[0][0] == v


def test_bool_roundtrip(cursor):
    b = True
    cursor.execute("SELECT cast(%s as bool) as f1", (b,))
    assert cursor.fetchall()[0][0] is b


def test_null_roundtrip(cursor):
    cursor.execute("select current_setting('server_version')")
    version = cursor.fetchall()[0][0][:2]

    if version.startswith('9'):
        # Prior to PostgreSQL version 10 We can't just "SELECT %s" and set
        # None as the parameter, since it has no type.  That would result
        # in a PG error, "could not determine data type of parameter %s".
        # So we create a temporary table, insert null values, and read them
        # back.
        cursor.execute(
            "CREATE TEMPORARY TABLE TestNullWrite "
            "(f1 int4, f2 timestamp, f3 varchar)")
        cursor.execute(
            "INSERT INTO TestNullWrite VALUES (%s, %s, %s)",
            (None, None, None))
        cursor.execute("SELECT * FROM TestNullWrite")
        assert cursor.fetchall()[0] == [None, None, None]

        with pytest.raises(pg8000.exceptions.DatabaseError):
            cursor.execute("SELECT %s as f1", (None,))
    else:
        cursor.execute("SELECT %s", (None,))
        assert cursor.fetchall()[0][0] is None


def test_decimal_roundtrip(cursor):
    values = (
        "1.1", "-1.1", "10000", "20000", "-1000000000.123456789", "1.0",
        "12.44")
    for v in values:
        cursor.execute("SELECT %s as f1", (decimal.Decimal(v),))
        retval = cursor.fetchall()
        assert str(retval[0][0]) == v


def test_float_roundtrip(cursor):
    val = 1.756e-12
    cursor.execute("SELECT cast(%s as double precision)", (val,))
    assert cursor.fetchall()[0][0] == val


def test_float_plus_infinity_roundtrip(cursor):
    v = float('inf')
    cursor.execute("SELECT cast(%s as double precision)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_str_roundtrip(cursor):
    v = "hello world"
    cursor.execute(
        "create temporary table test_str (f character varying(255))")
    cursor.execute("INSERT INTO test_str VALUES (%s)", (v,))
    cursor.execute("SELECT * from test_str")
    assert cursor.fetchall()[0][0] == v


def test_str_then_int(cursor):
    v1 = "hello world"
    cursor.execute("SELECT cast(%s as varchar) as f1", (v1,))
    assert cursor.fetchall()[0][0] == v1

    v2 = 1
    cursor.execute("SELECT cast(%s as varchar) as f1", (v2,))
    assert cursor.fetchall()[0][0] == str(v2)


def test_unicode_roundtrip(cursor):
    v = "hello \u0173 world"
    cursor.execute("SELECT cast(%s as varchar) as f1", (v,))
    assert cursor.fetchall()[0][0] == v


def test_long_roundtrip(cursor):
    v = 50000000000000
    cursor.execute("SELECT cast(%s as bigint)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_int_execute_many_select(cursor):
    cursor.executemany("SELECT %s", ((1,), (40000,)))
    cursor.fetchall()


def test_int_execute_many_insert(cursor):
    v = ([None], [4])
    cursor.execute("create temporary table test_int (f integer)")
    cursor.executemany("INSERT INTO test_int VALUES (%s)", v)
    cursor.execute("SELECT * from test_int")
    assert cursor.fetchall() == v


def test_insert_null(cursor):
    v = None
    cursor.execute("CREATE TEMPORARY TABLE test_int (f INTEGER)")
    cursor.execute("INSERT INTO test_int VALUES (%s)", (v,))
    cursor.execute("SELECT * FROM test_int")
    assert cursor.fetchall()[0][0] == v


def test_int_roundtrip(con, cursor):
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
        cursor.execute("SELECT cast(%s as " + MAP[typoid] + ")", (value,))
        assert cursor.fetchall()[0][0] == value
        column_name, column_typeoid = cursor.description[0][0:2]
        assert column_typeoid == typoid


def test_bytea_roundtrip(cursor):
    cursor.execute(
            "SELECT cast(%s as bytea)",
            (pg8000.Binary(b"\x00\x01\x02\x03\x02\x01\x00"),))
    assert cursor.fetchall()[0][0] == b"\x00\x01\x02\x03\x02\x01\x00"


def test_bytearray_round_trip(cursor):
    binary = b'\x00\x01\x02\x03\x02\x01\x00'
    cursor.execute("SELECT cast(%s as bytea)", (bytearray(binary),))
    assert cursor.fetchall()[0][0] == binary


def test_bytearray_subclass_round_trip(cursor):
    class BClass(bytearray):
        pass
    binary = b'\x00\x01\x02\x03\x02\x01\x00'
    cursor.execute("SELECT cast(%s as bytea)", (BClass(binary),))
    assert cursor.fetchall()[0][0] == binary


def test_timestamp_roundtrip(is_java, cursor):
    v = Datetime(2001, 2, 3, 4, 5, 6, 170000)
    cursor.execute("SELECT cast(%s as timestamp)", (v,))
    assert cursor.fetchall()[0][0] == v

    # Test that time zone doesn't affect it
    # Jython 2.5.3 doesn't have a time.tzset() so skip
    if not is_java:
        orig_tz = os.environ.get('TZ')
        os.environ['TZ'] = "America/Edmonton"
        time.tzset()

        cursor.execute("SELECT cast(%s as timestamp)", (v,))
        assert cursor.fetchall()[0][0] == v

        if orig_tz is None:
            del os.environ['TZ']
        else:
            os.environ['TZ'] = orig_tz
        time.tzset()


def test_interval_repr():
    v = pg8000.PGInterval(microseconds=123456789, days=2, months=24)
    assert repr(v) == '<PGInterval 24 months 2 days 123456789 microseconds>'


def test_interval_in_1_year():
    assert converters.pginterval_in('1 year') == pg8000.PGInterval(years=1)


def test_timedelta_in_2_months():
    assert converters.timedelta_in('2 hours')


def test_interval_roundtrip(con, cursor):
    con.register_in_adapter(1186, converters.pginterval_in)
    v = pg8000.PGInterval(microseconds=123456789, days=2, months=24)
    cursor.execute("SELECT cast(%s as interval)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_timedelta_roundtrip(cursor):
    v = Timedelta(seconds=30)
    cursor.execute("SELECT cast(%s as interval)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_enum_str_round_trip(cursor):
    try:
        cursor.execute(
            "create type lepton as enum ('electron', 'muon', 'tau')")

        v = 'muon'
        cursor.execute("SELECT cast(%s as lepton) as f1", (v,))
        retval = cursor.fetchall()
        assert retval[0][0] == v
        cursor.execute("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        cursor.execute(
            "INSERT INTO testenum VALUES (cast(%s as lepton))", ('electron',))
    finally:
        cursor.execute("drop table testenum")
        cursor.execute("drop type lepton")


def test_enum_custom_round_trip(con, cursor):
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
        cursor.execute("create type lepton as enum ('1', '2', '3')")
        cursor.execute("select oid from pg_type where typname = 'lepton'")
        lepton_oid = cursor.fetchall()[0][0]
        con.register_out_adapter(Lepton, lepton_oid, lepton_out)

        v = Lepton('muon', '2')
        cursor.execute("SELECT %s", (v,))
        assert cursor.fetchall()[0][0] == v.value
    finally:
        cursor.execute("drop type lepton")


def test_enum_py_round_trip(cursor):
    class Lepton(Enum):
        electron = '1'
        muon = '2'
        tau = '3'

    try:
        cursor.execute("create type lepton as enum ('1', '2', '3')")

        v = Lepton.muon
        cursor.execute("SELECT cast(%s as lepton) as f1", (v,))
        assert cursor.fetchall()[0][0] == v.value

        cursor.execute("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        cursor.execute(
            "INSERT INTO testenum VALUES (cast(%s as lepton))",
            (Lepton.electron,))
    finally:
        cursor.execute("drop table testenum")
        cursor.execute("drop type lepton")


def test_xml_roundtrip(cursor):
    v = '<genome>gatccgagtac</genome>'
    cursor.execute("select xmlparse(content %s) as f1", (v,))
    assert cursor.fetchall()[0][0] == v


def test_uuid_roundtrip(cursor):
    v = uuid.UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')
    cursor.execute("select cast(%s as uuid)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_inet_roundtrip_network(cursor):
    v = ipaddress.ip_network('192.168.0.0/28')
    cursor.execute("select cast(%s as inet)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_inet_roundtrip_address(cursor):
    v = ipaddress.ip_address('192.168.0.1')
    cursor.execute("select cast(%s as inet)", (v,))
    assert cursor.fetchall()[0][0] == v


def test_xid_roundtrip(cursor):
    v = 86722
    cursor.execute("select cast(cast(%s as varchar) as xid) as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v

    # Should complete without an exception
    cursor.execute("select * from pg_locks where transactionid = %s", (97712,))
    retval = cursor.fetchall()


def test_int2vector_in(cursor):
    cursor.execute("select cast('1 2' as int2vector) as f1")
    assert cursor.fetchall()[0][0] == [1, 2]

    # Should complete without an exception
    cursor.execute("select indkey from pg_index")
    cursor.fetchall()


def test_timestamp_tz_out(cursor):
    cursor.execute(
        "SELECT '2001-02-03 04:05:06.17 America/Edmonton'"
        "::timestamp with time zone")
    retval = cursor.fetchall()
    dt = retval[0][0]
    assert dt.tzinfo is not None, "no tzinfo returned"
    assert dt.astimezone(Timezone.utc) == Datetime(
        2001, 2, 3, 11, 5, 6, 170000, Timezone.utc), \
        "retrieved value match failed"


def test_timestamp_tz_roundtrip(is_java, cursor):
    if not is_java:
        mst = pytz.timezone("America/Edmonton")
        v1 = mst.localize(Datetime(2001, 2, 3, 4, 5, 6, 170000))
        cursor.execute("SELECT cast(%s as timestamptz)", (v1,))
        v2 = cursor.fetchall()[0][0]
        assert v2.tzinfo is not None
        assert v1 == v2


def test_timestamp_mismatch(is_java, cursor):
    if not is_java:
        mst = pytz.timezone("America/Edmonton")
        cursor.execute("SET SESSION TIME ZONE 'America/Edmonton'")
        try:
            cursor.execute(
                "CREATE TEMPORARY TABLE TestTz "
                "(f1 timestamp with time zone, "
                "f2 timestamp without time zone)")
            cursor.execute(
                "INSERT INTO TestTz (f1, f2) VALUES (%s, %s)", (
                    # insert timestamp into timestamptz field (v1)
                    Datetime(2001, 2, 3, 4, 5, 6, 170000),
                    # insert timestamptz into timestamp field (v2)
                    mst.localize(Datetime(
                        2001, 2, 3, 4, 5, 6, 170000))))
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
            assert f2 == Datetime(2001, 2, 3, 4, 5, 6, 170000)
        finally:
            cursor.execute("SET SESSION TIME ZONE DEFAULT")


def test_name_out(cursor):
    # select a field that is of "name" type:
    cursor.execute("SELECT usename FROM pg_user")
    cursor.fetchall()
    # It is sufficient that no errors were encountered.


def test_oid_out(cursor):
    cursor.execute("SELECT oid FROM pg_type")
    cursor.fetchall()
    # It is sufficient that no errors were encountered.


def test_boolean_in(cursor):
    cursor.execute("SELECT cast('t' as bool)")
    assert cursor.fetchall()[0][0]


def test_numeric_out(cursor):
    for num in ('5000', '50.34'):
        cursor.execute("SELECT " + num + "::numeric")
        assert str(cursor.fetchall()[0][0]) == num


def test_int2_out(cursor):
    cursor.execute("SELECT 5000::smallint")
    assert cursor.fetchall()[0][0] == 5000


def test_int4_out(cursor):
    cursor.execute("SELECT 5000::integer")
    assert cursor.fetchall()[0][0] == 5000


def test_int8_out(cursor):
    cursor.execute("SELECT 50000000000000::bigint")
    assert cursor.fetchall()[0][0] == 50000000000000


def test_float4_out(cursor):
    cursor.execute("SELECT 1.1::real")
    assert cursor.fetchall()[0][0] == 1.1


def test_float8_out(cursor):
    cursor.execute("SELECT 1.1::double precision")
    assert cursor.fetchall()[0][0] == 1.1000000000000001


def test_varchar_out(cursor):
    cursor.execute("SELECT 'hello'::varchar(20)")
    assert cursor.fetchall()[0][0] == "hello"


def test_char_out(cursor):
    cursor.execute("SELECT 'hello'::char(20)")
    assert cursor.fetchall()[0][0] == "hello               "


def test_text_out(cursor):
    cursor.execute("SELECT 'hello'::text")
    assert cursor.fetchall()[0][0] == "hello"


def test_interval_in(con, cursor):
    con.register_in_adapter(1186, pg8000.converters.pginterval_in)
    cursor.execute(
        "SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'::interval")
    expected_value = pg8000.PGInterval(
        microseconds=(12 * 60 * 60 * 1000 * 1000) +
        (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000), days=16, months=1)
    assert cursor.fetchall()[0][0] == expected_value


def test_interval_in_30_seconds(cursor):
    cursor.execute("select interval '30 seconds'")
    assert cursor.fetchall()[0][0] == Timedelta(seconds=30)


def test_interval_in_12_days_30_seconds(cursor):
    cursor.execute("select interval '12 days 30 seconds'")
    assert cursor.fetchall()[0][0] == Timedelta(days=12, seconds=30)


def test_timestamp_out(cursor):
    cursor.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
    retval = cursor.fetchall()
    assert retval[0][0] == Datetime(2001, 2, 3, 4, 5, 6, 170000)


def test_int4_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int2_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT2[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT2[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int8_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::INT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::INT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_bool_array_out(cursor):
    cursor.execute(
        "SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, "
        "'{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, "
        "'{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
        "::BOOL[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [True, False, False, True]
    assert f2 == [[True, False, True], [False, True, False]]
    assert f3 == [
        [[True, False], [False, True]], [[None, True], [False, False]]]


def test_float4_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::FLOAT4[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_float8_array_out(cursor):
    cursor.execute(
        "SELECT '{1,2,3,4}'::FLOAT8[] AS f1, "
        "'{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, "
        "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3")
    f1, f2, f3 = cursor.fetchone()
    assert f1 == [1, 2, 3, 4]
    assert f2 == [[1, 2, 3], [4, 5, 6]]
    assert f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]]


def test_int_array_roundtrip_small(cursor):
    """ send small int array, should be sent as INT2[]
    """
    cursor.execute("SELECT cast(%s as int2[])", ([1, 2, 3],))
    assert cursor.fetchall()[0][0], [1, 2, 3]
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"


def test_int_array_roundtrip_multi(cursor):
    """ test multi-dimensional array, should be sent as INT2[]
    """
    cursor.execute("SELECT cast(%s as int2[])", ([[1, 2], [3, 4]],))
    assert cursor.fetchall()[0][0] == [[1, 2], [3, 4]]

    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"


def test_int4_array_roundtrip(cursor):
    """ a larger value should kick it up to INT4[]...
    """
    cursor.execute("SELECT cast(%s as int4[])", ([70000, 2, 3],))
    assert cursor.fetchall()[0][0] == [70000, 2, 3]
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1007, "type should be INT4[]"


def test_int8_array_roundtrip(cursor):
    """ a much larger value should kick it up to INT8[]...
    """
    cursor.execute("SELECT cast(%s as int8[])", ([7000000000, 2, 3],))
    assert cursor.fetchall()[0][0] == [7000000000, 2, 3], \
        "retrieved value match failed"
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1016, "type should be INT8[]"


def test_int_array_with_null_roundtrip(cursor):
    cursor.execute("SELECT cast(%s as int[])", ([1, None, 3],))
    assert cursor.fetchall()[0][0] == [1, None, 3]


def test_float_array_roundtrip(cursor):
    cursor.execute("SELECT cast(%s as double precision[])", ([1.1, 2.2, 3.3],))
    assert cursor.fetchall()[0][0] == [1.1, 2.2, 3.3]


def test_bool_array_roundtrip(cursor):
    cursor.execute("SELECT cast(%s as bool[])", ([True, False, None],))
    assert cursor.fetchall()[0][0] == [True, False, None]


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("SELECT '{a,b,c}'::TEXT[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::CHAR[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::VARCHAR[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::CSTRING[] AS f1", ["a", "b", "c"]),
        ("SELECT '{a,b,c}'::NAME[] AS f1", ["a", "b", "c"]),
        ("SELECT '{}'::text[];", []),
        (
            "SELECT '{NULL,\"NULL\",NULL,\"\"}'::text[];",
            [None, 'NULL', None, ""]
        )
    ]
)
def test_string_array_out(cursor, test_input, expected):
    cursor.execute(test_input)
    assert cursor.fetchall()[0][0] == expected


def test_numeric_array_out(cursor):
    cursor.execute("SELECT '{1.1,2.2,3.3}'::numeric[] AS f1")
    assert cursor.fetchone()[0] == [
        decimal.Decimal("1.1"), decimal.Decimal("2.2"), decimal.Decimal("3.3")]


def test_numeric_array_roundtrip(cursor):
    v = [decimal.Decimal("1.1"), None, decimal.Decimal("3.3")]
    cursor.execute("SELECT cast(%s as numeric[])", (v,))
    assert cursor.fetchall()[0][0] == v


def test_string_array_roundtrip(cursor):
    v = [
        "Hello!", "World!", "abcdefghijklmnopqrstuvwxyz", "",
        "A bunch of random characters:",
        " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t", None]
    cursor.execute("SELECT cast(%s as varchar[])", (v,))
    assert cursor.fetchall()[0][0] == v


def test_array_string_escape():
    v = "\""
    res = pg8000.converters.array_string_escape(v)
    assert res == '"\\""'


def test_empty_array(cursor):
    v = []
    cursor.execute("SELECT cast(%s as varchar[])", (v,))
    assert cursor.fetchall()[0][0] == v


def test_macaddr(cursor):
    cursor.execute("SELECT macaddr '08002b:010203'")
    assert cursor.fetchall()[0][0] == "08:00:2b:01:02:03"


def test_tsvector_roundtrip(cursor):
    cursor.execute(
        "SELECT cast(%s as tsvector)",
        ('a fat cat sat on a mat and ate a fat rat',))
    retval = cursor.fetchall()
    assert retval[0][0] == "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"


def test_hstore_roundtrip(cursor):
    val = '"a"=>"1"'
    cursor.execute("SELECT cast(%s as hstore)", (val,))
    assert cursor.fetchall()[0][0] == val


def test_json_roundtrip(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as jsonb)", (dumps(val),))
    assert cursor.fetchall()[0][0] == val


def test_jsonb_roundtrip(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as jsonb)", (dumps(val),))
    retval = cursor.fetchall()
    assert retval[0][0] == val


def test_json_access_object(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as json) -> %s", (dumps(val), 'name'))
    retval = cursor.fetchall()
    assert retval[0][0] == 'Apollo 11 Cave'


def test_jsonb_access_object(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as jsonb) -> %s", (dumps(val), 'name'))
    retval = cursor.fetchall()
    assert retval[0][0] == 'Apollo 11 Cave'


def test_json_access_array(cursor):
    val = [-1, -2, -3, -4, -5]
    cursor.execute(
        "SELECT cast(%s as json) -> cast(%s as int)", (dumps(val), 2))
    assert cursor.fetchall()[0][0] == -3


def test_jsonb_access_array(cursor):
    val = [-1, -2, -3, -4, -5]
    cursor.execute(
        "SELECT cast(%s as jsonb) -> cast(%s as int)", (dumps(val), 2))
    assert cursor.fetchall()[0][0] == -3


def test_jsonb_access_path(cursor):
    j = {
        "a": [1, 2, 3],
        "b": [4, 5, 6]}

    path = ['a', '2']

    cursor.execute("SELECT cast(%s as jsonb) #>> %s", (dumps(j), path))
    assert cursor.fetchall()[0][0] == str(j[path[0]][int(path[1])])


def test_infinity_timestamp_roundtrip(cursor):
    v = 'infinity'
    cursor.execute("SELECT cast(%s as timestamp) as f1", (v,))
    assert cursor.fetchall()[0][0] == v


def test_point_roundtrip(cursor):
    v = '(2.3,1)'
    cursor.execute("SELECT cast(%s as point) as f1", (v,))
    assert cursor.fetchall()[0][0] == v


def test_time_in():
    actual = pg8000.converters.time_in("12:57:18.000396")
    assert actual == Time(12, 57, 18, 396)
