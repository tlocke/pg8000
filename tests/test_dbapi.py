from __future__ import with_statement

import unittest
import pg8000
import datetime
from contextlib import closing, nested
from .connection_settings import db_connect

dbapi = pg8000.DBAPI
db2 = dbapi.connect(**db_connect)


# DBAPI compatible interface tests
class Tests(unittest.TestCase):
    def setUp(self):
        with closing(db2.cursor()) as c:
            try:
                c.execute("DROP TABLE t1")
            except pg8000.DatabaseError, e:
                # the only acceptable error is:
                self.assert_(
                    e.args[1] == '42P01',  # table does not exist
                    "incorrect error for drop table")
            c.execute(
                "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
                "f2 int not null, f3 varchar(50) null)")
            c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (1, 1, None))
            c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (2, 10, None))
            c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (3, 100, None))
            c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (4, 1000, None))
            c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (5, 10000, None))

    def testParallelQueries(self):
        with nested(closing(db2.cursor()), closing(db2.cursor())) as (c1, c2):
            c1.execute("SELECT f1, f2, f3 FROM t1")
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
                c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
                while 1:
                    row = c2.fetchone()
                    if row is None:
                        break
                    f1, f2, f3 = row

    def testFormat(self):
        with closing(db2.cursor()) as c1:
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row

    def testPyformat(self):
        with closing(db2.cursor()) as c1:
            c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row

    def testArraysize(self):
        with closing(db2.cursor()) as c1:
            c1.arraysize = 3
            c1.execute("SELECT * FROM t1")
            retval = c1.fetchmany()
            self.assert_(
                len(retval) == c1.arraysize,
                "fetchmany returned wrong number of rows")

    def testDate(self):
        val = dbapi.Date(2001, 2, 3)
        self.assert_(
            val == datetime.date(2001, 2, 3),
            "Date constructor value match failed")

    def testTime(self):
        val = dbapi.Time(4, 5, 6)
        self.assert_(
            val == datetime.time(4, 5, 6),
            "Time constructor value match failed")

    def testTimestamp(self):
        val = dbapi.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assert_(
            val == datetime.datetime(2001, 2, 3, 4, 5, 6),
            "Timestamp constructor value match failed")

    def testDateFromTicks(self):
        val = dbapi.DateFromTicks(1173804319)
        self.assert_(
            val == datetime.date(2007, 3, 13),
            "DateFromTicks constructor value match failed")

    def testTimeFromTicks(self):
        val = dbapi.TimeFromTicks(1173804319)
        self.assert_(
            val == datetime.time(10, 45, 19),
            "TimeFromTicks constructor value match failed")

    def testTimestampFromTicks(self):
        val = dbapi.TimestampFromTicks(1173804319)
        self.assert_(
            val == datetime.datetime(2007, 3, 13, 10, 45, 19),
            "TimestampFromTicks constructor value match failed")

    def testBinary(self):
        v = dbapi.Binary("\x00\x01\x02\x03\x02\x01\x00")
        self.assert_(
            v == "\x00\x01\x02\x03\x02\x01\x00", "Binary value match failed")
        self.assert_(
            isinstance(v, pg8000.Bytea), "Binary type match failed")

    def testRowCount(self):
        with closing(db2.cursor()) as c1:
            c1.execute("SELECT * FROM t1")
            self.assertEquals(-1, c1.rowcount)

            c1.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
            self.assertEquals(2, c1.rowcount)

            c1.execute("DELETE FROM t1")
            self.assertEquals(5, c1.rowcount)

    def testFetchMany(self):
        with closing(db2.cursor()) as cursor:
            cursor.arraysize = 2
            cursor.execute("SELECT * FROM t1")
            self.assertEquals(2, len(cursor.fetchmany()))
            self.assertEquals(2, len(cursor.fetchmany()))
            self.assertEquals(1, len(cursor.fetchmany()))
            self.assertEquals(0, len(cursor.fetchmany()))

    def testIterator(self):
        from warnings import filterwarnings
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")

        with closing(db2.cursor()) as cursor:
            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            f1 = 0
            for row in cursor:
                next_f1 = row[0]
                assert next_f1 > f1
                f1 = next_f1

    def test_incorrect_positional_param_scalar(self):
        with closing(db2.cursor()) as cursor:
            self.assertRaises(
                dbapi.ProgrammingError,
                cursor.execute,
                "select %s",
                "not a tuple"
            )

    def test_incorrect_positional_param_too_few(self):
        with closing(db2.cursor()) as cursor:
            self.assertRaises(
                dbapi.ProgrammingError,
                cursor.execute,
                "select %s %s %s",
                (1, 2)
            )

    def test_incorrect_positional_param_too_many(self):
        with closing(db2.cursor()) as cursor:
            self.assertRaises(
                dbapi.ProgrammingError,
                cursor.execute,
                "select %s %s",
                (1, 2, 3, 4, 5)
            )

    def test_incorrect_dict_param_initial(self):
        with closing(db2.cursor()) as cursor:
            self.assertRaises(
                dbapi.ProgrammingError,
                cursor.execute,
                "select %(param1)s %(param2)s",
                {"q": "hi", "p": "there"}
            )

    def test_incorrect_dict_param_later(self):
        with closing(db2.cursor()) as cursor:
            self.assertRaises(
                dbapi.ProgrammingError,
                cursor.executemany,
                "select %(param1)s %(param2)s",
                [
                    {"param1": "hi", "param2": "there"},
                    {"q": "hi", "p": "there"}
                ]


            )

if __name__ == "__main__":
    unittest.main()
