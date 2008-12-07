import unittest
from pg8000 import errors, types, dbapi
import datetime
import decimal
import struct
from .connection_settings import db_connect

db = dbapi.connect(**db_connect)
cursor = db.cursor()

# Type conversion tests
class Tests(unittest.TestCase):
    def testTimeRoundtrip(self):
        cursor.execute("SELECT %s as f1", (datetime.time(4, 5, 6),))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == datetime.time(4, 5, 6),
                "retrieved value match failed")

    def testDateRoundtrip(self):
        cursor.execute("SELECT %s as f1", (datetime.date(2001, 2, 3),))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == datetime.date(2001, 2, 3),
                "retrieved value match failed")

    def testBoolRoundtrip(self):
        cursor.execute("SELECT %s as f1", (True,))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == True,
                "retrieved value match failed")

    def testNullRoundtrip(self):
        # We can't just "SELECT %s" and set None as the parameter, since it has
        # no type.  That would result in a PG error, "could not determine data
        # type of parameter %s".  So we create a temporary table, insert null
        # values, and read them back.
        cursor.execute("CREATE TEMPORARY TABLE TestNullWrite (f1 int4, f2 timestamp, f3 varchar)")
        cursor.execute("INSERT INTO TestNullWrite VALUES (%s, %s, %s)",
                (None, None, None,))
        cursor.execute("SELECT * FROM TestNullWrite")
        retval = cursor.fetchone()
        self.assert_(retval == (None, None, None),
                "retrieved value match failed")

    def testNullSelectFailure(self):
        # See comment in TestNullRoundtrip.  This test is here to ensure that
        # this behaviour is documented and doesn't mysteriously change.
        self.assertRaises(errors.ProgrammingError, cursor.execute,
                "SELECT %s as f1", (None,))

    def testDecimalRoundtrip(self):
        cursor.execute("SELECT %s as f1", (decimal.Decimal('1.1'),))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == decimal.Decimal('1.1'),
                "retrieved value match failed")

    def testFloatRoundtrip(self):
        # This test ensures that the binary float value doesn't change in a
        # roundtrip to the server.  That could happen if the value was
        # converted to text and got rounded by a decimal place somewhere.
        val = 1.756e-12
        bin_orig = struct.pack("!d", val)
        cursor.execute("SELECT %s as f1", (val,))
        retval = cursor.fetchall()
        bin_new = struct.pack("!d", retval[0][0])
        self.assert_(bin_new == bin_orig,
                "retrieved value match failed")

    def testStrRoundtrip(self):
        cursor.execute("SELECT %s as f1", ("hello world",))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "hello world",
                "retrieved value match failed")

    def testUnicodeRoundtrip(self):
        cursor.execute("SELECT %s as f1", ("hello \u0173 world",))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "hello \u0173 world",
                "retrieved value match failed")

    def testLongRoundtrip(self):
        cursor.execute("SELECT %s as f1", (50000000000000,))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 50000000000000,
                "retrieved value match failed")

    def testIntRoundtrip(self):
        cursor.execute("SELECT %s as f1", (100,))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 100,
                "retrieved value match failed")

    def testByteaRoundtrip(self):
        cursor.execute("SELECT %s as f1", (dbapi.Binary("\x00\x01\x02\x03\x02\x01\x00"),))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "\x00\x01\x02\x03\x02\x01\x00",
                "retrieved value match failed")

    def testTimestampRoundtrip(self):
        v = datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)
        cursor.execute("SELECT %s as f1", (v,))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == v,
                "retrieved value match failed")

    def testIntervalRoundtrip(self):
        v = types.Interval(microseconds=123456789, days=2, months=24)
        cursor.execute("SELECT %s as f1", (v,))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == v,
                "retrieved value match failed")

    def testTimestampTzOut(self):
        cursor.execute("SELECT '2001-02-03 04:05:06.17 America/Edmonton'::timestamp with time zone")
        retval = cursor.fetchall()
        dt = retval[0][0]
        self.assert_(dt.tzinfo != None, "no tzinfo returned")
        self.assert_(
                dt.astimezone(types.utc) ==
                datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, types.utc),
                "retrieved value match failed")

    def testTimestampTzRoundtrip(self):
        import pytz
        mst = pytz.timezone("America/Edmonton")
        v1 = mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
        cursor.execute("SELECT %s as f1", (v1,))
        retval = cursor.fetchall()
        v2 = retval[0][0]
        self.assert_(v2.tzinfo != None, "expected tzinfo on v2")
        self.assert_(v1 == v2, "expected v1 == v2")

    def testTimestampMismatch(self):
        import pytz
        mst = pytz.timezone("America/Edmonton")
        cursor.execute("SET SESSION TIME ZONE 'America/Edmonton'")
        try:
            cursor.execute("CREATE TEMPORARY TABLE TestTz (f1 timestamp with time zone, f2 timestamp without time zone)")
            cursor.execute("INSERT INTO TestTz (f1, f2) VALUES (%s, %s)", (
                    # insert timestamp into timestamptz field (v1)
                    datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                    # insert timestamptz into timestamp field (v2)
                    mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
                )
            )
            cursor.execute("SELECT f1, f2 FROM TestTz")
            retval = cursor.fetchall()

            # when inserting a timestamp into a timestamptz field, postgresql
            # assumes that it is in local time.  So the value that comes out
            # will be the server's local time interpretation of v1.  We've set
            # the server's TZ to MST, the time should be...
            f1 = retval[0][0]
            self.assert_(f1 == datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, pytz.utc),
                    "retrieved value match failed")

            # inserting the timestamptz into a timestamp field, pg8000
            # converts the value into UTC, and then the PG server converts
            # it into local time for insertion into the field.  When we query
            # for it, we get the same time back, like the tz was dropped.
            f2 = retval[0][1]
            self.assert_(f2 == datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                    "retrieved value match failed")
        finally:
            cursor.execute("SET SESSION TIME ZONE DEFAULT")


    def testNameOut(self):
        # select a field that is of "name" type:
        cursor.execute("SELECT usename FROM pg_user")
        retval = cursor.fetchall()
        # It is sufficient that no errors were encountered.

    def testOidOut(self):
        cursor.execute("SELECT oid FROM pg_type")
        retval = cursor.fetchall()
        # It is sufficient that no errors were encountered.

    def testBooleanOut(self):
        cursor.execute("SELECT 't'::bool")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == True,
                "retrieved value match failed")

    def testNumericOut(self):
        cursor.execute("SELECT 5000::numeric")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == decimal.Decimal("5000"),
                "retrieved value match failed")

    def testInt2Out(self):
        cursor.execute("SELECT 5000::smallint")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 5000,
                "retrieved value match failed")

    def testInt4Out(self):
        cursor.execute("SELECT 5000::integer")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 5000,
                "retrieved value match failed")

    def testInt8Out(self):
        cursor.execute("SELECT 50000000000000::bigint")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 50000000000000,
                "retrieved value match failed")

    def testFloat4Out(self):
        cursor.execute("SELECT 1.1::real")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 1.1000000238418579,
                "retrieved value match failed")

    def testFloat8Out(self):
        cursor.execute("SELECT 1.1::double precision")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == 1.1000000000000001,
                "retrieved value match failed")

    def testVarcharOut(self):
        cursor.execute("SELECT 'hello'::varchar(20)")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "hello",
                "retrieved value match failed")

    def testCharOut(self):
        cursor.execute("SELECT 'hello'::char(20)")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "hello               ",
                "retrieved value match failed")

    def testTextOut(self):
        cursor.execute("SELECT 'hello'::text")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "hello",
                "retrieved value match failed")

    def testIntervalOut(self):
        cursor.execute("SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'::interval")
        retval = cursor.fetchall()
        expected_value = types.Interval(
                microseconds = (12 * 60 * 60 * 1000 * 1000) + (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000),
                days = 16,
                months = 1)
        self.assert_(retval[0][0] == expected_value,
                "retrieved value match failed")

    def testTimestampOut(self):
        cursor.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                "retrieved value match failed")

    # confirms that pg8000's binary output methods have the same output for
    # a data type as the PG server
    def testBinaryOutputMethods(self):
        methods = (
                ("float8send", 22.2),
                ("timestamp_send", datetime.datetime(2001, 2, 3, 4, 5, 6, 789)),
                ("byteasend", dbapi.Binary("\x01\x02")),
                ("interval_send", types.Interval(1234567, 123, 123)),
        )
        for method_out, value in methods:
            cursor.execute("SELECT %s(%%s) as f1" % method_out, (value,))
            retval = cursor.fetchall()
            self.assert_(retval[0][0] == getattr(types, method_out)(value, integer_datetimes=db.conn.c._integer_datetimes))

    def testInt4ArrayOut(self):
        cursor.execute("SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt2ArrayOut(self):
        cursor.execute("SELECT '{1,2,3,4}'::INT2[] AS f1, '{{1,2,3},{4,5,6}}'::INT2[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt8ArrayOut(self):
        cursor.execute("SELECT '{1,2,3,4}'::INT8[] AS f1, '{{1,2,3},{4,5,6}}'::INT8[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testBoolArrayOut(self):
        cursor.execute("SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, '{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, '{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'::BOOL[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [True, False, False, True])
        self.assert_(f2 == [[True, False, True], [False, True, False]])
        self.assert_(f3 == [[[True, False], [False, True]], [[None, True], [False, False]]])

    def testFloat4ArrayOut(self):
        cursor.execute("SELECT '{1,2,3,4}'::FLOAT4[] AS f1, '{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testFloat8ArrayOut(self):
        cursor.execute("SELECT '{1,2,3,4}'::FLOAT8[] AS f1, '{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3")
        f1, f2, f3 = cursor.fetchone()
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testIntArrayRoundtrip(self):
        cursor.execute("SELECT %s as f1", ([1, 2, 3],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == [1, 2, 3],
                "retrieved value match failed")

    def testIntArrayWithNullRoundtrip(self):
        cursor.execute("SELECT %s as f1", ([1, None, 3],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == [1, None, 3],
                "retrieved value match failed")

    def testFloatArrayRoundtrip(self):
        cursor.execute("SELECT %s as f1", ([1.1, 2.2, 3.3],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == [1.1, 2.2, 3.3],
                "retrieved value match failed")

    def testBoolArrayRoundtrip(self):
        cursor.execute("SELECT %s as f1", ([True, False, None],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == [True, False, None],
                "retrieved value match failed")

    def testStringArrayOut(self):
        cursor.execute("SELECT '{a,b,c}'::TEXT[] AS f1")
        self.assert_(cursor.fetchone()[0] == ["a", "b", "c"])
        cursor.execute("SELECT '{a,b,c}'::CHAR[] AS f1")
        self.assert_(cursor.fetchone()[0] == ["a", "b", "c"])
        cursor.execute("SELECT '{a,b,c}'::VARCHAR[] AS f1")
        self.assert_(cursor.fetchone()[0] == ["a", "b", "c"])
        cursor.execute("SELECT '{a,b,c}'::CSTRING[] AS f1")
        self.assert_(cursor.fetchone()[0] == ["a", "b", "c"])
        cursor.execute("SELECT '{a,b,c}'::NAME[] AS f1")
        self.assert_(cursor.fetchone()[0] == ["a", "b", "c"])

    def testStringArrayRoundtrip(self):
        cursor.execute("SELECT %s as f1", (["Hello!", "World!", None],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == ["Hello!", "World!", None],
                "retrieved value match failed")

        cursor.execute("SELECT %s as f1", (["Hello!", "World!", None],))
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == ["Hello!", "World!", None],
                "retrieved value match failed")

    def testArrayHasValue(self):
        self.assertRaises(errors.ArrayContentEmptyError,
                types.array_inspect, [[None],[None],[None]])

    def testArrayContentNotSupported(self):
        # someday we might support numeric arrays, but we'd need binary
        # support for the numeric format, which we don't have
        self.assertRaises(errors.ArrayContentNotSupportedError,
                types.array_inspect, [[decimal.Decimal("1.1")],[None],[None]])

    def testArrayDimensions(self):
        self.assertRaises(errors.ArrayDimensionsNotConsistentError,
                types.array_inspect, [1,[2]])
        self.assertRaises(errors.ArrayDimensionsNotConsistentError,
                types.array_inspect, [[1],[2],[3,4]])
        self.assertRaises(errors.ArrayDimensionsNotConsistentError,
                types.array_inspect, [[[1]],[[2]],[[3,4]]])
        self.assertRaises(errors.ArrayDimensionsNotConsistentError,
                types.array_inspect, [[[[1]]],[[[2]]],[[[3,4]]]])
        self.assertRaises(errors.ArrayDimensionsNotConsistentError,
                types.array_inspect, [[1,2,3],[4,[5],6]])

    def testArrayHomogenous(self):
        self.assertRaises(errors.ArrayContentNotHomogenousError,
                types.array_inspect, [[[1]],[[2]],[[3.1]]])

    def testArrayInspect(self):
        types.array_inspect([1,2,3])
        types.array_inspect([[1],[2],[3]])
        types.array_inspect([[[1]],[[2]],[[3]]])

    def testMacaddr(self):
        cursor.execute("SELECT macaddr '08002b:010203'")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == "08:00:2b:01:02:03",
                "retrieved value match failed")

    def testUserType(self):
        try:
            cursor.execute("DROP TYPE test_type")
        except errors.DatabaseError as e:
            self.assert_(e.args[1] == '42704', # type does not exist
                "incorrect error for drop type")
        cursor.execute("CREATE TYPE test_type AS (a INT, b FLOAT)")
        db.recache_record_types()
        cursor.execute("SELECT ROW(1, 2)::test_type")
        retval = cursor.fetchall()
        self.assert_(retval[0][0] == {"a": 1, "b": 2},
                "retrieved value match failed")

if __name__ == "__main__":
    unittest.main()

