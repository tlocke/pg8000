import unittest
import os
import time
import pg8000
import datetime
from .connection_settings import db_connect
from sys import exc_info
from pg8000.six import b, IS_JYTHON
from distutils.version import LooseVersion


# DBAPI compatible interface tests
class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)
        # Jython 2.5.3 doesn't have a time.tzset() so skip
        if not IS_JYTHON:
            os.environ['TZ'] = "UTC"
            time.tzset()

        try:
            c = self.db.cursor()
            try:
                c = self.db.cursor()
                c.execute("DROP TABLE t1")
            except pg8000.DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(e.args[1], b('42P01'))  # table does not exist
                self.db.rollback()
            c.execute(
                "CREATE TEMPORARY TABLE t1 "
                "(f1 int primary key, f2 int not null, f3 varchar(50) null)")
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
            self.db.commit()
        finally:
            c.close()

    def tearDown(self):
        self.db.close()

    def testParallelQueries(self):
        try:
            c1 = self.db.cursor()
            c2 = self.db.cursor()

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
        finally:
            c1.close()
            c2.close()

        self.db.rollback()

    def testQmark(self):
        orig_paramstyle = pg8000.paramstyle
        try:
            pg8000.paramstyle = "qmark"
            c1 = self.db.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            self.db.rollback()
        finally:
            pg8000.paramstyle = orig_paramstyle
            c1.close()

    def testNumeric(self):
        orig_paramstyle = pg8000.paramstyle
        try:
            pg8000.paramstyle = "numeric"
            c1 = self.db.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            self.db.rollback()
        finally:
            pg8000.paramstyle = orig_paramstyle
            c1.close()

    def testNamed(self):
        orig_paramstyle = pg8000.paramstyle
        try:
            pg8000.paramstyle = "named"
            c1 = self.db.cursor()
            c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            self.db.rollback()
        finally:
            pg8000.paramstyle = orig_paramstyle
            c1.close()

    def testFormat(self):
        orig_paramstyle = pg8000.paramstyle
        try:
            pg8000.paramstyle = "format"
            c1 = self.db.cursor()
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            self.db.commit()
        finally:
            pg8000.paramstyle = orig_paramstyle
            c1.close()

    def testPyformat(self):
        orig_paramstyle = pg8000.paramstyle
        try:
            pg8000.paramstyle = "pyformat"
            c1 = self.db.cursor()
            c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            self.db.commit()
        finally:
            pg8000.paramstyle = orig_paramstyle
            c1.close()

    def testArraysize(self):
        try:
            c1 = self.db.cursor()
            c1.arraysize = 3
            c1.execute("SELECT * FROM t1")
            retval = c1.fetchmany()
            self.assertEqual(len(retval), c1.arraysize)
        finally:
            c1.close()
        self.db.commit()

    def testDate(self):
        val = pg8000.Date(2001, 2, 3)
        self.assertEqual(val, datetime.date(2001, 2, 3))

    def testTime(self):
        val = pg8000.Time(4, 5, 6)
        self.assertEqual(val, datetime.time(4, 5, 6))

    def testTimestamp(self):
        val = pg8000.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assertEqual(val, datetime.datetime(2001, 2, 3, 4, 5, 6))

    def testDateFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.DateFromTicks(1173804319)
        self.assertEqual(val, datetime.date(2007, 3, 13))

    def testTimeFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.TimeFromTicks(1173804319)
        self.assertEqual(val, datetime.time(16, 45, 19))

    def testTimestampFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.TimestampFromTicks(1173804319)
        self.assertEqual(val, datetime.datetime(2007, 3, 13, 16, 45, 19))

    def testBinary(self):
        v = pg8000.Binary(b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertEqual(v, b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertTrue(isinstance(v, pg8000.BINARY))

    def testRowCount(self):
        try:
            c1 = self.db.cursor()
            c1.execute("SELECT * FROM t1")

            # Before PostgreSQL 9 we don't know the row count for a select
            if self.db._server_version > LooseVersion('8.0.0'):
                self.assertEqual(5, c1.rowcount)

            c1.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
            self.assertEqual(2, c1.rowcount)

            c1.execute("DELETE FROM t1")
            self.assertEqual(5, c1.rowcount)
        finally:
            c1.close()
        self.db.commit()

    def testFetchMany(self):
        try:
            cursor = self.db.cursor()
            cursor.arraysize = 2
            cursor.execute("SELECT * FROM t1")
            self.assertEqual(2, len(cursor.fetchmany()))
            self.assertEqual(2, len(cursor.fetchmany()))
            self.assertEqual(1, len(cursor.fetchmany()))
            self.assertEqual(0, len(cursor.fetchmany()))
        finally:
            cursor.close()
        self.db.commit()

    def testIterator(self):
        from warnings import filterwarnings
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")

        try:
            cursor = self.db.cursor()
            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            f1 = 0
            for row in cursor:
                next_f1 = row[0]
                assert next_f1 > f1
                f1 = next_f1
        except:
            cursor.close()

        self.db.commit()

    # Vacuum can't be run inside a transaction, so we need to turn
    # autocommit on.
    def testVacuum(self):
        self.db.autocommit = True
        try:
            cursor = self.db.cursor()
            cursor.execute("vacuum")
        finally:
            cursor.close()

    # If autocommit is on and we do an operation that returns more rows than
    # the cache holds, make sure exception raised.
    def testAutocommitMaxRows(self):
        self.db.autocommit = True
        try:
            cursor = self.db.cursor()
            self.assertRaises(
                pg8000.InterfaceError, cursor.execute,
                "select generate_series(1, " +
                str(pg8000.core.Connection._row_cache_size + 1) + ")")
        finally:
            cursor.close()

if __name__ == "__main__":
    unittest.main()
