import unittest
import pg8000
import datetime
import decimal
import struct
from connection_settings import db_connect
db = pg8000.Connection(**db_connect)

# Type conversion tests
class Tests(unittest.TestCase):
    def testTimeRoundtrip(self):
        db.execute("SELECT $1 as f1", datetime.time(4, 5, 6))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": datetime.time(4, 5, 6)},),
                "retrieved value match failed")

    def testDateRoundtrip(self):
        db.execute("SELECT $1 as f1", datetime.date(2001, 2, 3))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": datetime.date(2001, 2, 3)},),
                "retrieved value match failed")

    def testBoolRoundtrip(self):
        db.execute("SELECT $1 as f1", True)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": True},),
                "retrieved value match failed")

    def testNullRoundtrip(self):
        # We can't just "SELECT $1" and set None as the parameter, since it has
        # no type.  That would result in a PG error, "could not determine data
        # type of parameter $1".  So we create a temporary table, insert null
        # values, and read them back.
        db.execute("CREATE TEMPORARY TABLE TestNullWrite (f1 int4, f2 timestamp, f3 varchar)")
        db.execute("INSERT INTO TestNullWrite VALUES ($1, $2, $3)",
                None, None, None)
        db.execute("SELECT * FROM TestNullWrite")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": None, "f2": None, "f3": None},),
                "retrieved value match failed")

    def testNullSelectFailure(self):
        # See comment in TestNullRoundtrip.  This test is here to ensure that
        # this behaviour is documented and doesn't mysteriously change.
        self.assertRaises(pg8000.ProgrammingError, db.execute,
                "SELECT $1 as f1", None)

    def testDecimalRoundtrip(self):
        db.execute("SELECT $1 as f1", decimal.Decimal('1.1'))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": decimal.Decimal('1.1')},),
                "retrieved value match failed")

    def testFloatRoundtrip(self):
        # This test ensures that the binary float value doesn't change in a
        # roundtrip to the server.  That could happen if the value was
        # converted to text and got rounded by a decimal place somewhere.
        val = 1.756e-12
        bin_orig = struct.pack("!d", val)
        db.execute("SELECT $1 as f1", val)
        retval = tuple(db.iterate_dict())
        bin_new = struct.pack("!d", retval[0]['f1'])
        self.assert_(bin_new == bin_orig,
                "retrieved value match failed")

    def testStrRoundtrip(self):
        db.execute("SELECT $1 as f1", "hello world")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": u"hello world"},),
                "retrieved value match failed")

    def testUnicodeRoundtrip(self):
        db.execute("SELECT $1 as f1", u"hello \u0173 world")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": u"hello \u0173 world"},),
                "retrieved value match failed")

    def testLongRoundtrip(self):
        db.execute("SELECT $1 as f1", 50000000000000L)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": 50000000000000L},),
                "retrieved value match failed")

    def testIntRoundtrip(self):
        db.execute("SELECT $1 as f1", 100)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": 100},),
                "retrieved value match failed")

    def testByteaRoundtrip(self):
        db.execute("SELECT $1 as f1", pg8000.Bytea("\x00\x01\x02\x03\x02\x01\x00"))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": "\x00\x01\x02\x03\x02\x01\x00"},),
                "retrieved value match failed")

    def testTimestampRoundtrip(self):
        v = datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)
        db.execute("SELECT $1 as f1", v)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": v},),
                "retrieved value match failed")

    def testIntervalRoundtrip(self):
        v = pg8000.types.Interval(microseconds=123456789, days=2, months=24)
        db.execute("SELECT $1 as f1", v)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": v},),
                "retrieved value match failed")

    def testTimestampTzOut(self):
        db.execute("SELECT '2001-02-03 04:05:06.17 America/Edmonton'::timestamp with time zone")
        retval = tuple(db.iterate_dict())
        dt = retval[0]['timestamptz']
        self.assert_(dt.tzinfo != None, "no tzinfo returned")
        self.assert_(
                dt.astimezone(pg8000.types.utc) ==
                datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, pg8000.types.utc),
                "retrieved value match failed")

    def testTimestampTzRoundtrip(self):
        import pytz
        mst = pytz.timezone("America/Edmonton")
        v1 = mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
        db.execute("SELECT $1 as f1", v1)
        retval = tuple(db.iterate_dict())
        v2 = retval[0]['f1']
        self.assert_(v2.tzinfo != None, "expected tzinfo on v2")
        self.assert_(v1 == v2, "expected v1 == v2")

    def testTimestampMismatch(self):
        import pytz
        mst = pytz.timezone("America/Edmonton")
        db.execute("SET SESSION TIME ZONE 'America/Edmonton'")
        try:
            db.execute("CREATE TEMPORARY TABLE TestTz (f1 timestamp with time zone, f2 timestamp without time zone)")
            db.execute("INSERT INTO TestTz (f1, f2) VALUES ($1, $2)",
                    # insert timestamp into timestamptz field (v1)
                    datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                    # insert timestamptz into timestamp field (v2)
                    mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
                )
            db.execute("SELECT f1, f2 FROM TestTz")
            retval = tuple(db.iterate_dict())

            # when inserting a timestamp into a timestamptz field, postgresql
            # assumes that it is in local time.  So the value that comes out
            # will be the server's local time interpretation of v1.  We've set
            # the server's TZ to MST, the time should be...
            f1 = retval[0]['f1']
            self.assert_(f1 == datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, pytz.utc),
                    "retrieved value match failed")

            # inserting the timestamptz into a timestamp field, pg8000
            # converts the value into UTC, and then the PG server converts
            # it into local time for insertion into the field.  When we query
            # for it, we get the same time back, like the tz was dropped.
            f2 = retval[0]['f2']
            self.assert_(f2 == datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                    "retrieved value match failed")
        finally:
            db.execute("SET SESSION TIME ZONE DEFAULT")


    def testNameOut(self):
        # select a field that is of "name" type:
        db.execute("SELECT usename FROM pg_user")
        retval = tuple(db.iterate_dict())
        # It is sufficient that no errors were encountered.

    def testOidOut(self):
        db.execute("SELECT oid FROM pg_type")
        retval = tuple(db.iterate_dict())
        # It is sufficient that no errors were encountered.

    def testBooleanOut(self):
        db.execute("SELECT 't'::bool")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"bool": True},),
                "retrieved value match failed")

    def testNumericOut(self):
        db.execute("SELECT 5000::numeric")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"numeric": decimal.Decimal("5000")},),
                "retrieved value match failed")

    def testInt2Out(self):
        db.execute("SELECT 5000::smallint")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int2": 5000},),
                "retrieved value match failed")

    def testInt4Out(self):
        db.execute("SELECT 5000::integer")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int4": 5000},),
                "retrieved value match failed")

    def testInt8Out(self):
        db.execute("SELECT 50000000000000::bigint")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int8": 50000000000000},),
                "retrieved value match failed")

    def testFloat4Out(self):
        db.execute("SELECT 1.1::real")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"float4": 1.1000000238418579},),
                "retrieved value match failed")

    def testFloat8Out(self):
        db.execute("SELECT 1.1::double precision")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"float8": 1.1000000000000001},),
                "retrieved value match failed")

    def testVarcharOut(self):
        db.execute("SELECT 'hello'::varchar(20)")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"varchar": u"hello"},),
                "retrieved value match failed")

    def testCharOut(self):
        db.execute("SELECT 'hello'::char(20)")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"bpchar": u"hello               "},),
                "retrieved value match failed")

    def testTextOut(self):
        db.execute("SELECT 'hello'::text")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"text": u"hello"},),
                "retrieved value match failed")

    def testIntervalOut(self):
        db.execute("SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'::interval")
        retval = tuple(db.iterate_dict())
        expected_value = pg8000.types.Interval(
                microseconds = (12 * 60 * 60 * 1000 * 1000) + (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000),
                days = 16,
                months = 1)
        self.assert_(retval == ({"interval": expected_value},),
                "retrieved value match failed")

    def testTimestampOut(self):
        db.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"timestamp": datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)},),
                "retrieved value match failed")

    # confirms that pg8000's binary output methods have the same output for
    # a data type as the PG server
    def testBinaryOutputMethods(self):
        from pg8000 import types
        methods = (
                ("float8send", 22.2),
                ("timestamp_send", datetime.datetime(2001, 2, 3, 4, 5, 6, 789)),
                ("byteasend", pg8000.Bytea("\x01\x02")),
                ("interval_send", pg8000.types.Interval(1234567, 123, 123)),
        )
        for method_out, value in methods:
            db.execute("SELECT %s($1) as f1" % method_out, value)
            retval = tuple(db.iterate_dict())
            self.assert_(retval[0]["f1"] == getattr(types, method_out)(value, integer_datetimes=db.c._integer_datetimes))

    def testInt4ArrayOut(self):
        db.execute("SELECT '{1,2,3,4}'::INT[] AS f1, '{{1,2,3},{4,5,6}}'::INT[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt2ArrayOut(self):
        db.execute("SELECT '{1,2,3,4}'::INT2[] AS f1, '{{1,2,3},{4,5,6}}'::INT2[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt8ArrayOut(self):
        db.execute("SELECT '{1,2,3,4}'::INT8[] AS f1, '{{1,2,3},{4,5,6}}'::INT8[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testBoolArrayOut(self):
        db.execute("SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, '{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, '{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'::BOOL[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [True, False, False, True])
        self.assert_(f2 == [[True, False, True], [False, True, False]])
        self.assert_(f3 == [[[True, False], [False, True]], [[None, True], [False, False]]])

    def testFloat4ArrayOut(self):
        db.execute("SELECT '{1,2,3,4}'::FLOAT4[] AS f1, '{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testFloat8ArrayOut(self):
        db.execute("SELECT '{1,2,3,4}'::FLOAT8[] AS f1, '{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, '{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3")
        f1, f2, f3 = tuple(db.iterate_tuple())[0]
        self.assert_(f1 == [1, 2, 3, 4])
        self.assert_(f2 == [[1, 2, 3], [4, 5, 6]])
        self.assert_(f3 == [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testIntArrayRoundtrip(self):
        db.execute("SELECT $1 as f1", [1, 2, 3])
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": [1, 2, 3]},),
                "retrieved value match failed")

    def testIntArrayWithNullRoundtrip(self):
        db.execute("SELECT $1 as f1", [1, None, 3])
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": [1, None, 3]},),
                "retrieved value match failed")

    def testFloatArrayRoundtrip(self):
        db.execute("SELECT $1 as f1", [1.1, 2.2, 3.3])
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": [1.1, 2.2, 3.3]},),
                "retrieved value match failed")

    def testBoolArrayRoundtrip(self):
        db.execute("SELECT $1 as f1", [True, False, None])
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": [True, False, None]},),
                "retrieved value match failed")

    def testArrayHasValue(self):
        from pg8000 import errors, types
        self.assertRaises(errors.ArrayContentEmptyError,
                types.array_inspect, [[None],[None],[None]])

    def testArrayContentNotSupported(self):
        from pg8000 import errors, types
        # someday we might support string arrays, but not yet
        self.assertRaises(errors.ArrayContentNotSupportedError,
                types.array_inspect, [["Hello!"],["Hello!"],["Hello!"]])

    def testArrayDimensions(self):
        from pg8000 import errors, types
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
        from pg8000 import errors, types
        self.assertRaises(errors.ArrayContentNotHomogenousError,
                types.array_inspect, [[[1]],[[2]],[[3.1]]])

    def testArrayInspect(self):
        from pg8000 import types
        types.array_inspect([1,2,3])
        types.array_inspect([[1],[2],[3]])
        types.array_inspect([[[1]],[[2]],[[3]]])


if __name__ == "__main__":
    unittest.main()

