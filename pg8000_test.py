#!/usr/bin/env python

import datetime
import decimal
import threading
import unittest
import struct

import pg8000
dbapi = pg8000.DBAPI

db_joy_connect = {
        "host": "joy",
        "user": "pg8000-test",
        "database": "pg8000-test",
        "password": "pg8000-test",
        "socket_timeout": 5,
        "ssl": False,
        }

db_local_connect = {
        "unix_sock": "/tmp/.s.PGSQL.5432",
        "user": "mfenniak"
        }

db_connect = db_local_connect

db = pg8000.Connection(**db_connect)
db2 = dbapi.connect(**db_connect)


# Tests related to connecting to a database.
class ConnectionTests(unittest.TestCase):
    def TestUnixSockFailure(self):
        self.assertRaises(pg8000.InterfaceError, pg8000.Connection,
                unix_sock="/file-does-not-exist", user="doesn't-matter")

    def TestDatabaseMissing(self):
        self.assertRaises(pg8000.ProgrammingError, pg8000.Connection,
                unix_sock=db_local_connect['unix_sock'], database='missing-db',
                user='mfenniak')


# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.
class QueryTests(unittest.TestCase):
    def setUp(self):
        try:
            db.execute("DROP TABLE t1")
        except pg8000.DatabaseError as e:
            # the only acceptable error is:
            self.assert_(e.args[1] == b'42P01', # table does not exist
                    "incorrect error for drop table")
        db.execute("CREATE TEMPORARY TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")

    def TestDatabaseError(self):
        self.assertRaises(pg8000.ProgrammingError, db.execute, "INSERT INTO t99 VALUES (1, 2, 3)")

    def TestParallelQueries(self):
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 1, 1, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 2, 10, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 3, 100, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 4, 1000, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 5, 10000, None)
        c1 = pg8000.Cursor(db)
        c2 = pg8000.Cursor(db)
        c1.execute("SELECT f1, f2, f3 FROM t1")
        for row in c1.iterate_tuple():
            f1, f2, f3 = row
            c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > $1", f1)
            for row in c2.iterate_tuple():
                f1, f2, f3 = row

    def TestNoDataErrorRecovery(self):
        for i in range(1, 4):
            try:
                db.execute("DROP TABLE t1")
            except pg8000.DatabaseError as e:
                # the only acceptable error is:
                self.assert_(e.args[1] == b'42P01', # table does not exist
                        "incorrect error for drop table")

    def TestMultithreadedStatement(self):
        # Note: Multithreading with a prepared statement is not highly
        # recommended due to low performance.
        s1 = pg8000.PreparedStatement(db, "INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", int, int, str)
        def test(left, right):
            for i in range(left, right):
                s1.execute(i, id(threading.currentThread()), None)
        t1 = threading.Thread(target=test, args=(1, 25))
        t2 = threading.Thread(target=test, args=(25, 50))
        t3 = threading.Thread(target=test, args=(50, 75))
        t1.start(); t2.start(); t3.start()
        t1.join(); t2.join(); t3.join()

    def TestMultithreadedCursor(self):
        # Note: Multithreading with a cursor is not highly recommended due to
        # low performance.
        cur = pg8000.Cursor(db)
        def test(left, right):
            for i in range(left, right):
                cur.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", i, id(threading.currentThread()), None)
        t1 = threading.Thread(target=test, args=(1, 25))
        t2 = threading.Thread(target=test, args=(25, 50))
        t3 = threading.Thread(target=test, args=(50, 75))
        t1.start(); t2.start(); t3.start()
        t1.join(); t2.join(); t3.join()


# Tests of the convert_paramstyle function.
class ParamstyleTests(unittest.TestCase):
    def TestQmark(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("qmark", "SELECT ?, ?, \"field_?\" FROM t WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'", (1, 2, 3))
        assert new_query == "SELECT $1, $2, \"field_?\" FROM t WHERE a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'"
        assert new_args == (1, 2, 3)

    def TestQmark2(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("qmark", "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'", (1, 2, 3))
        assert new_query == "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'"
        assert new_args == (1, 2, 3)

    def TestNumeric(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("numeric", "SELECT :2, :1, * FROM t WHERE a=:3", (1, 2, 3))
        assert new_query == "SELECT $2, $1, * FROM t WHERE a=$3"
        assert new_args == (1, 2, 3)

    def TestNamed(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("named", "SELECT :f2, :f1 FROM t WHERE a=:f2", {"f2": 1, "f1": 2})
        assert new_query == "SELECT $1, $2 FROM t WHERE a=$1"
        assert new_args == (1, 2)

    def TestFormat(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("format", "SELECT %s, %s, \"f1_%%\", E'txt_%%' FROM t WHERE a=%s AND b='75%%'", (1, 2, 3))
        assert new_query == "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$3 AND b='75%'"
        assert new_args == (1, 2, 3)

    def TestPyformat(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle("pyformat", "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' FROM t WHERE a=%(f2)s AND b='75%%'", {"f2": 1, "f1": 2, "f3": 3})
        assert new_query == "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$1 AND b='75%'"
        assert new_args == (1, 2)


# DBAPI compatible interface tests
class DBAPITests(unittest.TestCase):
    def setUp(self):
        c = db2.cursor()
        try:
            c.execute("DROP TABLE t1")
        except pg8000.DatabaseError as e:
            # the only acceptable error is:
            self.assert_(e.args[1] == b'42P01', # table does not exist
                    "incorrect error for drop table")
        c.execute("CREATE TEMPORARY TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))

    def TestParallelQueries(self):
        c1 = db2.cursor()
        c2 = db2.cursor()
        c1.execute("SELECT f1, f2, f3 FROM t1")
        while 1:
            row = c1.fetchone()
            if row == None:
                break
            f1, f2, f3 = row
            c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
            while 1:
                row = c2.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row

    def TestQmark(self):
        orig_paramstyle = dbapi.paramstyle
        try:
            dbapi.paramstyle = "qmark"
            c1 = db2.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (3,))
            while 1:
                row = c1.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row
        finally:
            dbapi.paramstyle = orig_paramstyle

    def TestNumeric(self):
        orig_paramstyle = dbapi.paramstyle
        try:
            dbapi.paramstyle = "numeric"
            c1 = db2.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
            while 1:
                row = c1.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row
        finally:
            dbapi.paramstyle = orig_paramstyle

    def TestNamed(self):
        orig_paramstyle = dbapi.paramstyle
        try:
            dbapi.paramstyle = "named"
            c1 = db2.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row
        finally:
            dbapi.paramstyle = orig_paramstyle

    def TestFormat(self):
        orig_paramstyle = dbapi.paramstyle
        try:
            dbapi.paramstyle = "format"
            c1 = db2.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
            while 1:
                row = c1.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row
        finally:
            dbapi.paramstyle = orig_paramstyle
    
    def TestPyformat(self):
        orig_paramstyle = dbapi.paramstyle
        try:
            dbapi.paramstyle = "pyformat"
            c1 = db2.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row == None:
                    break
                f1, f2, f3 = row
        finally:
            dbapi.paramstyle = orig_paramstyle

    def TestArraysize(self):
        c1 = db2.cursor()
        c1.arraysize = 3
        c1.execute("SELECT * FROM t1")
        retval = c1.fetchmany()
        self.assert_(len(retval) == c1.arraysize,
                "fetchmany returned wrong number of rows")

    def TestDate(self):
        val = dbapi.Date(2001, 2, 3)
        self.assert_(val == datetime.date(2001, 2, 3),
                "Date constructor value match failed")

    def TestTime(self):
        val = dbapi.Time(4, 5, 6)
        self.assert_(val == datetime.time(4, 5, 6),
                "Time constructor value match failed")

    def TestTimestamp(self):
        val = dbapi.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assert_(val == datetime.datetime(2001, 2, 3, 4, 5, 6),
                "Timestamp constructor value match failed")

    def TestDateFromTicks(self):
        val = dbapi.DateFromTicks(1173804319)
        self.assert_(val == datetime.date(2007, 3, 13),
                "DateFromTicks constructor value match failed")

    def TestTimeFromTicks(self):
        val = dbapi.TimeFromTicks(1173804319)
        self.assert_(val == datetime.time(10, 45, 19),
                "TimeFromTicks constructor value match failed")

    def TestTimestampFromTicks(self):
        val = dbapi.TimestampFromTicks(1173804319)
        self.assert_(val == datetime.datetime(2007, 3, 13, 10, 45, 19),
                "TimestampFromTicks constructor value match failed")

    def TestBinary(self):
        v = dbapi.Binary(b"\x00\x01\x02\x03\x02\x01\x00")
        self.assert_(v == b"\x00\x01\x02\x03\x02\x01\x00",
                "Binary value match failed")
        self.assert_(isinstance(v, pg8000.Bytea),
                "Binary type match failed")


# Tests relating to type conversion.
class TypeTests(unittest.TestCase):
    def TestTimeRoundtrip(self):
        db.execute("SELECT $1 as f1", datetime.time(4, 5, 6))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": datetime.time(4, 5, 6)},),
                "retrieved value match failed")

    def TestDateRoundtrip(self):
        db.execute("SELECT $1 as f1", datetime.date(2001, 2, 3))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": datetime.date(2001, 2, 3)},),
                "retrieved value match failed")

    def TestBoolRoundtrip(self):
        db.execute("SELECT $1 as f1", True)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": True},),
                "retrieved value match failed")

    def TestNullRoundtrip(self):
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

    def TestNullSelectFailure(self):
        # See comment in TestNullRoundtrip.  This test is here to ensure that
        # this behaviour is documented and doesn't mysteriously change.
        self.assertRaises(pg8000.ProgrammingError, db.execute,
                "SELECT $1 as f1", None)

    def TestDecimalRoundtrip(self):
        db.execute("SELECT $1 as f1", decimal.Decimal('1.1'))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": decimal.Decimal('1.1')},),
                "retrieved value match failed")

    def TestFloatRoundtrip(self):
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

    def TestStrRoundtrip(self):
        db.execute("SELECT $1 as f1", "hello world")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": "hello world"},),
                "retrieved value match failed")

    def TestUnicodeRoundtrip(self):
        db.execute("SELECT $1 as f1", "hello \u0173 world")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": "hello \u0173 world"},),
                "retrieved value match failed")

    def TestLongRoundtrip(self):
        db.execute("SELECT $1 as f1", 50000000000000)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": 50000000000000},),
                "retrieved value match failed")

    def TestIntRoundtrip(self):
        db.execute("SELECT $1 as f1", 100)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": 100},),
                "retrieved value match failed")

    def TestByteaRoundtrip(self):
        db.execute("SELECT $1 as f1", pg8000.Bytea(b"\x00\x01\x02\x03\x02\x01\x00"))
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": b"\x00\x01\x02\x03\x02\x01\x00"},),
                "retrieved value match failed")

    def TestTimestampRoundtrip(self):
        v = datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)
        db.execute("SELECT $1 as f1", v)
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"f1": v},),
                "retrieved value match failed")

    def TestTimestampTzOut(self):
        db.execute("SELECT '2001-02-03 04:05:06.17 Canada/Mountain'::timestamp with time zone")
        retval = tuple(db.iterate_dict())
        dt = retval[0]['timestamptz']
        self.assert_(dt.tzinfo.hrs == -7,
                "timezone hrs != -7")
        self.assert_(
                datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond) ==
                datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                "retrieved value match failed")

    def TestNameOut(self):
        # select a field that is of "name" type:
        db.execute("SELECT usename FROM pg_user")
        retval = tuple(db.iterate_dict())
        # It is sufficient that no errors were encountered.

    def TestOidOut(self):
        db.execute("SELECT oid FROM pg_type")
        retval = tuple(db.iterate_dict())
        # It is sufficient that no errors were encountered.

    def TestBooleanOut(self):
        db.execute("SELECT 't'::bool")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"bool": True},),
                "retrieved value match failed")

    def TestNumericOut(self):
        db.execute("SELECT 5000::numeric")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"numeric": decimal.Decimal("5000")},),
                "retrieved value match failed")

    def TestInt2Out(self):
        db.execute("SELECT 5000::smallint")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int2": 5000},),
                "retrieved value match failed")

    def TestInt4Out(self):
        db.execute("SELECT 5000::integer")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int4": 5000},),
                "retrieved value match failed")

    def TestInt8Out(self):
        db.execute("SELECT 50000000000000::bigint")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"int8": 50000000000000},),
                "retrieved value match failed")

    def TestFloat4Out(self):
        db.execute("SELECT 1.1::real")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"float4": 1.1000000238418579},),
                "retrieved value match failed")

    def TestFloat8Out(self):
        db.execute("SELECT 1.1::double precision")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"float8": 1.1000000000000001},),
                "retrieved value match failed")

    def TestVarcharOut(self):
        db.execute("SELECT 'hello'::varchar(20)")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"varchar": "hello"},),
                "retrieved value match failed")

    def TestCharOut(self):
        db.execute("SELECT 'hello'::char(20)")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"bpchar": "hello               "},),
                "retrieved value match failed")

    def TestTextOut(self):
        db.execute("SELECT 'hello'::text")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"text": "hello"},),
                "retrieved value match failed")

    def TestIntervalOut(self):
        db.execute("SELECT '1 month'::interval")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"interval": b"1 mon"},),
                "retrieved value match failed")

    def TestTimestampOut(self):
        db.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
        retval = tuple(db.iterate_dict())
        self.assert_(retval == ({"timestamp": datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)},),
                "retrieved value match failed")


def suite():
    connection_tests = unittest.makeSuite(ConnectionTests, "Test")
    paramstyle_tests = unittest.makeSuite(ParamstyleTests, "Test")
    dbapi_tests = unittest.makeSuite(DBAPITests, "Test")
    query_tests = unittest.makeSuite(QueryTests, "Test")
    type_tests = unittest.makeSuite(TypeTests, "Test")

    return unittest.TestSuite((
        connection_tests,
        paramstyle_tests,
        dbapi_tests,
        query_tests,
        type_tests,
    ))

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())

