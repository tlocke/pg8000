import pg8000
from pg8000 import PGJsonb, PGEnum
from datetime import (
    datetime as Datetime, time as Time, date as Date, timedelta as Timedelta,
    timezone as Timezone)
import decimal
import struct
import uuid
import os
import time
import json
import pytz
from collections import OrderedDict
import pytest
from enum import Enum
import ipaddress


# Type conversion tests

def testTimeRoundtrip(cursor):
    cursor.execute("SELECT %s as f1", (Time(4, 5, 6),))
    retval = cursor.fetchall()
    assert retval[0][0] == Time(4, 5, 6)


def test_date_roundtrip(cursor):
    v = Date(2001, 2, 3)
    cursor.execute("SELECT %s as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v


def test_bool_roundtrip(cursor):
    cursor.execute("SELECT %s as f1", (True,))
    retval = cursor.fetchall()
    assert retval[0][0] is True


def test_null_roundtrip(cursor):
    cursor.execute("show server_version_num")
    version = cursor.fetchone()[0][:2]

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
            (None, None, None,))
        cursor.execute("SELECT * FROM TestNullWrite")
        retval = cursor.fetchone()
        assert retval == [None, None, None]

        with pytest.raises(pg8000.ProgrammingError):
            cursor.execute("SELECT %s as f1", (None,))
    else:
        cursor.execute("SELECT %s as f1", (None,))
        assert cursor.fetchone()[0] is None


def test_decimal_roundtrip(cursor):
    values = (
        "1.1", "-1.1", "10000", "20000", "-1000000000.123456789", "1.0",
        "12.44")
    for v in values:
        cursor.execute("SELECT %s as f1", (decimal.Decimal(v),))
        retval = cursor.fetchall()
        assert str(retval[0][0]) == v


def test_float_roundtrip(cursor):
    # This test ensures that the binary float value doesn't change in a
    # roundtrip to the server.  That could happen if the value was
    # converted to text and got rounded by a decimal place somewhere.
    val = 1.756e-12
    bin_orig = struct.pack("!d", val)
    cursor.execute("SELECT %s as f1", (val,))
    retval = cursor.fetchall()
    bin_new = struct.pack("!d", retval[0][0])
    assert bin_new == bin_orig


def test_float_plus_infinity_roundtrip(cursor):
    v = float('inf')
    cursor.execute("SELECT %s as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v


def test_str_roundtrip(cursor):
    v = "hello world"
    cursor.execute(
        "create temporary table test_str (f character varying(255))")
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


def test_long_roundtrip(cursor):
    v = 50000000000000
    retval = tuple(cursor.execute("SELECT %s", (v,)))
    assert retval[0][0] == v


def test_int_execute_many(cursor):
    tuple(cursor.executemany("SELECT %s", ((1,), (40000,))))

    v = ([None], [4])
    cursor.execute("create temporary table test_int (f integer)")
    cursor.executemany("INSERT INTO test_int VALUES (%s)", v)
    retval = tuple(cursor.execute("SELECT * from test_int"))
    assert retval == v


def test_int_roundtrip(cursor):
    int2 = 21
    int4 = 23
    int8 = 20

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
        cursor.execute("SELECT %s", (value,))
        retval = cursor.fetchall()
        assert retval[0][0] == value
        column_name, column_typeoid = cursor.description[0][0:2]
        assert column_typeoid == typoid


def test_bytea_roundtrip(cursor):
    cursor.execute(
        "SELECT %s as f1", (pg8000.Binary(b"\x00\x01\x02\x03\x02\x01\x00"),))
    retval = cursor.fetchall()
    assert retval[0][0] == b"\x00\x01\x02\x03\x02\x01\x00"


def test_bytearray_round_trip(cursor):
    binary = b'\x00\x01\x02\x03\x02\x01\x00'
    cursor.execute("SELECT %s as f1", (bytearray(binary),))
    retval = cursor.fetchall()
    assert retval[0][0] == binary


def test_bytearray_subclass_round_trip(cursor):
    class BClass(bytearray):
        pass
    binary = b'\x00\x01\x02\x03\x02\x01\x00'
    cursor.execute("SELECT %s as f1", (BClass(binary),))
    retval = cursor.fetchall()
    assert retval[0][0] == binary


def test_timestamp_roundtrip(is_java, cursor):
    v = Datetime(2001, 2, 3, 4, 5, 6, 170000)
    cursor.execute("SELECT %s as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v

    # Test that time zone doesn't affect it
    # Jython 2.5.3 doesn't have a time.tzset() so skip
    if not is_java:
        orig_tz = os.environ.get('TZ')
        os.environ['TZ'] = "America/Edmonton"
        time.tzset()

        cursor.execute("SELECT %s as f1", (v,))
        retval = cursor.fetchall()
        assert retval[0][0] == v

        if orig_tz is None:
            del os.environ['TZ']
        else:
            os.environ['TZ'] = orig_tz
        time.tzset()


def test_interval_roundtrip(cursor):
    v = pg8000.Interval(microseconds=123456789, days=2, months=24)
    cursor.execute("SELECT %s as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v

    v = Timedelta(seconds=30)
    cursor.execute("SELECT %s as f1", (v,))
    retval = cursor.fetchall()
    assert retval[0][0] == v


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


def test_enum_custom_round_trip(cursor):
    class Lepton(object):
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

    try:
        cursor.execute("create type lepton as enum ('1', '2', '3')")

        v = Lepton('muon', '2')
        cursor.execute("SELECT cast(%s as lepton) as f1", (PGEnum(v),))
        retval = cursor.fetchall()
        assert retval[0][0] == v.value
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
        retval = tuple(cursor.execute("SELECT cast(%s as lepton) as f1", (v,)))
        assert retval[0][0] == v.value

        cursor.execute("CREATE TEMPORARY TABLE testenum (f1 lepton)")
        cursor.execute(
            "INSERT INTO testenum VALUES (cast(%s as lepton))",
            (Lepton.electron,))
    finally:
        cursor.execute("drop table testenum")
        cursor.execute("drop type lepton")


def test_xml_roundtrip(cursor):
    v = '<genome>gatccgagtac</genome>'
    retval = tuple(cursor.execute("select xmlparse(content %s) as f1", (v,)))
    assert retval[0][0] == v


def test_uuid_roundtrip(cursor):
    v = uuid.UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')
    retval = tuple(cursor.execute("select %s as f1", (v,)))
    assert retval[0][0] == v


def test_inet_roundtrip(cursor):
    v = ipaddress.ip_network('192.168.0.0/28')
    retval = tuple(cursor.execute("select %s as f1", (v,)))
    assert retval[0][0] == v

    v = ipaddress.ip_address('192.168.0.1')
    retval = tuple(cursor.execute("select %s as f1", (v,)))
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
        cursor.execute("SELECT %s as f1", (v1,))
        retval = cursor.fetchall()
        v2 = retval[0][0]
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
    tuple(cursor.execute("SELECT usename FROM pg_user"))
    # It is sufficient that no errors were encountered.


def test_oid_out(cursor):
    tuple(cursor.execute("SELECT oid FROM pg_type"))
    # It is sufficient that no errors were encountered.


def test_boolean_out(cursor):
    retval = tuple(cursor.execute("SELECT cast('t' as bool)"))
    assert retval[0][0]


def test_numeric_out(cursor):
    for num in ('5000', '50.34'):
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
    assert retval[0][0] == 1.1000000238418579


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


def test_interval_out(cursor):
    retval = tuple(
        cursor.execute(
            "SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'"
            "::interval"))
    expected_value = pg8000.Interval(
        microseconds=(12 * 60 * 60 * 1000 * 1000) +
        (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000), days=16, months=1)
    assert retval[0][0] == expected_value

    retval = tuple(cursor.execute("select interval '30 seconds'"))
    assert retval[0][0] == Timedelta(seconds=30)

    retval = tuple(cursor.execute("select interval '12 days 30 seconds'"))
    assert retval[0][0] == Timedelta(days=12, seconds=30)


def test_timestamp_out(cursor):
    cursor.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
    retval = cursor.fetchall()
    assert retval[0][0] == Datetime(2001, 2, 3, 4, 5, 6, 170000)


# confirms that pg8000's binary output methods have the same output for
# a data type as the PG server
def test_binary_output_methods(con):
    with con.cursor() as cursor:
        methods = (
            ("float8send", 22.2),
            ("timestamp_send", Datetime(2001, 2, 3, 4, 5, 6, 789)),
            ("byteasend", pg8000.Binary(b"\x01\x02")),
            ("interval_send", pg8000.Interval(1234567, 123, 123)),)
        for method_out, value in methods:
            cursor.execute("SELECT %s(%%s) as f1" % method_out, (value,))
            retval = cursor.fetchall()
            assert retval[0][0] == con.make_params((value,))[0][2](value)


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


def test_int_array_roundtrip(cursor):
    # send small int array, should be sent as INT2[]
    retval = tuple(cursor.execute("SELECT %s as f1", ([1, 2, 3],)))
    assert retval[0][0], [1, 2, 3]
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"

    # test multi-dimensional array, should be sent as INT2[]
    retval = tuple(cursor.execute("SELECT %s as f1", ([[1, 2], [3, 4]],)))
    assert retval[0][0] == [[1, 2], [3, 4]]

    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1005, "type should be INT2[]"

    # a larger value should kick it up to INT4[]...
    cursor.execute("SELECT %s as f1 -- integer[]", ([70000, 2, 3],))
    retval = cursor.fetchall()
    assert retval[0][0] == [70000, 2, 3]
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1007, "type should be INT4[]"

    # a much larger value should kick it up to INT8[]...
    cursor.execute("SELECT %s as f1 -- bigint[]", ([7000000000, 2, 3],))
    retval = cursor.fetchall()
    assert retval[0][0] == [7000000000, 2, 3], "retrieved value match failed"
    column_name, column_typeoid = cursor.description[0][0:2]
    assert column_typeoid == 1016, "type should be INT8[]"


def test_int_array_with_null_roundtrip(cursor):
    retval = tuple(cursor.execute("SELECT %s as f1", ([1, None, 3],)))
    assert retval[0][0] == [1, None, 3]


def test_float_array_roundtrip(cursor):
    retval = tuple(cursor.execute("SELECT %s as f1", ([1.1, 2.2, 3.3],)))
    assert retval[0][0] == [1.1, 2.2, 3.3]


def test_bool_array_roundtrip(cursor):
    retval = tuple(cursor.execute("SELECT %s as f1", ([True, False, None],)))
    assert retval[0][0] == [True, False, None]


def test_string_array_out(cursor):
    cursor.execute("SELECT '{a,b,c}'::TEXT[] AS f1")
    assert cursor.fetchone()[0] == ["a", "b", "c"]
    cursor.execute("SELECT '{a,b,c}'::CHAR[] AS f1")
    assert cursor.fetchone()[0] == ["a", "b", "c"]
    cursor.execute("SELECT '{a,b,c}'::VARCHAR[] AS f1")
    assert cursor.fetchone()[0] == ["a", "b", "c"]
    cursor.execute("SELECT '{a,b,c}'::CSTRING[] AS f1")
    assert cursor.fetchone()[0] == ["a", "b", "c"]
    cursor.execute("SELECT '{a,b,c}'::NAME[] AS f1")
    assert cursor.fetchone()[0] == ["a", "b", "c"]
    cursor.execute("SELECT '{}'::text[];")
    assert cursor.fetchone()[0] == []
    cursor.execute("SELECT '{NULL,\"NULL\",NULL,\"\"}'::text[];")
    assert cursor.fetchone()[0] == [None, 'NULL', None, ""]


def test_numeric_array_out(cursor):
    cursor.execute("SELECT '{1.1,2.2,3.3}'::numeric[] AS f1")
    assert cursor.fetchone()[0] == [
        decimal.Decimal("1.1"), decimal.Decimal("2.2"), decimal.Decimal("3.3")]


def test_numeric_array_roundtrip(cursor):
    v = [decimal.Decimal("1.1"), None, decimal.Decimal("3.3")]
    retval = tuple(cursor.execute("SELECT %s as f1", (v,)))
    assert retval[0][0] == v


def test_string_array_roundtrip(cursor):
    v = [
        "Hello!", "World!", "abcdefghijklmnopqrstuvwxyz", "",
        "A bunch of random characters:",
        " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t", None]
    retval = tuple(cursor.execute("SELECT %s as f1", (v,)))
    assert retval[0][0] == v


def test_empty_array(cursor):
    v = []
    retval = tuple(cursor.execute("SELECT %s as f1", (v,)))
    assert retval[0][0] == v


def test_array_content_not_supported(con):
    class Kajigger(object):
        pass
    with pytest.raises(pg8000.ArrayContentNotSupportedError):
        con.array_inspect([[Kajigger()], [None], [None]])


def test_array_dimensions(con):
    for arr in (
            [1, [2]], [[1], [2], [3, 4]],
            [[[1]], [[2]], [[3, 4]]],
            [[[1]], [[2]], [[3, 4]]],
            [[[[1]]], [[[2]]], [[[3, 4]]]],
            [[1, 2, 3], [4, [5], 6]]):

        arr_send = con.array_inspect(arr)[2]
        with pytest.raises(pg8000.ArrayDimensionsNotConsistentError):
            arr_send(arr)


def test_array_homogenous(con):
    arr = [[[1]], [[2]], [[3.1]]]
    arr_send = con.array_inspect(arr)[2]
    with pytest.raises(pg8000.ArrayContentNotHomogenousError):
        arr_send(arr)


def test_array_inspect(con):
    con.array_inspect([1, 2, 3])
    con.array_inspect([[1], [2], [3]])
    con.array_inspect([[[1]], [[2]], [[3]]])


def test_macaddr(cursor):
    retval = tuple(cursor.execute("SELECT macaddr '08002b:010203'"))
    assert retval[0][0] == "08:00:2b:01:02:03"


def test_tsvector_roundtrip(cursor):
    cursor.execute(
        "SELECT cast(%s as tsvector)",
        ('a fat cat sat on a mat and ate a fat rat',))
    retval = cursor.fetchall()
    assert retval[0][0] == "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"


def test_hstore_roundtrip(cursor):
    val = '"a"=>"1"'
    retval = tuple(cursor.execute("SELECT cast(%s as hstore)", (val,)))
    assert retval[0][0] == val


def test_json_roundtrip(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    retval = tuple(cursor.execute("SELECT %s", (pg8000.PGJson(val),)))
    assert retval[0][0] == val


def test_jsonb_roundtrip(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as jsonb)", (json.dumps(val),))
    retval = cursor.fetchall()
    assert retval[0][0] == val


def test_json_access_object(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as json) -> %s", (json.dumps(val), 'name'))
    retval = cursor.fetchall()
    assert retval[0][0] == 'Apollo 11 Cave'


def test_jsonb_access_object(cursor):
    val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
    cursor.execute("SELECT cast(%s as jsonb) -> %s", (json.dumps(val), 'name'))
    retval = cursor.fetchall()
    assert retval[0][0] == 'Apollo 11 Cave'


def test_json_access_array(cursor):
    val = [-1, -2, -3, -4, -5]
    cursor.execute("SELECT cast(%s as json) -> %s", (json.dumps(val), 2))
    retval = cursor.fetchall()
    assert retval[0][0] == -3


def test_jsonb_access_array(cursor):
    val = [-1, -2, -3, -4, -5]
    cursor.execute("SELECT cast(%s as jsonb) -> %s", (json.dumps(val), 2))
    retval = cursor.fetchall()
    assert retval[0][0] == -3


def test_jsonb_access_path(cursor):
    j = {
        "a": [1, 2, 3],
        "b": [4, 5, 6]}

    path = ['a', '2']

    retval = tuple(cursor.execute("SELECT %s #>> %s", [PGJsonb(j), path]))
    assert retval[0][0] == str(j[path[0]][int(path[1])])


def test_timestamp_send_float():
    assert b'A\xbe\x19\xcf\x80\x00\x00\x00' == \
        pg8000.core.timestamp_send_float(Datetime(2016, 1, 2, 0, 0))


def test_infinity_timestamp_roundtrip(cursor):
    v = 'infinity'
    retval = tuple(cursor.execute("SELECT cast(%s as timestamp) as f1", (v,)))
    assert retval[0][0] == v


def test_point_roundtrip(cursor):
    v = '(2.3,1)'
    retval = tuple(cursor.execute("SELECT cast(%s as point) as f1", (v,)))
    assert retval[0][0] == v


def test_array_dim_lengths():
    assert pg8000.core.array_dim_lengths([[4], [5]]) == [2, 1]
