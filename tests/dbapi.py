import unittest
import pg8000
import datetime
from connection_settings import db_connect

dbapi = pg8000.DBAPI
db2 = dbapi.connect(**db_connect)

# DBAPI compatible interface tests
class Tests(unittest.TestCase):
    def setUp(self):
        c = db2.cursor()
        try:
            c.execute("DROP TABLE t1")
        except pg8000.DatabaseError, e:
            # the only acceptable error is:
            self.assert_(e.args[1] == '42P01', # table does not exist
                    "incorrect error for drop table")
        c.execute("CREATE TEMPORARY TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
        c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))

    def testParallelQueries(self):
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

    def testQmark(self):
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

    def testNumeric(self):
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

    def testNamed(self):
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

    def testFormat(self):
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
    
    def testPyformat(self):
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

    def testArraysize(self):
        c1 = db2.cursor()
        c1.arraysize = 3
        c1.execute("SELECT * FROM t1")
        retval = c1.fetchmany()
        self.assert_(len(retval) == c1.arraysize,
                "fetchmany returned wrong number of rows")

    def testDate(self):
        val = dbapi.Date(2001, 2, 3)
        self.assert_(val == datetime.date(2001, 2, 3),
                "Date constructor value match failed")

    def testTime(self):
        val = dbapi.Time(4, 5, 6)
        self.assert_(val == datetime.time(4, 5, 6),
                "Time constructor value match failed")

    def testTimestamp(self):
        val = dbapi.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assert_(val == datetime.datetime(2001, 2, 3, 4, 5, 6),
                "Timestamp constructor value match failed")

    def testDateFromTicks(self):
        val = dbapi.DateFromTicks(1173804319)
        self.assert_(val == datetime.date(2007, 3, 13),
                "DateFromTicks constructor value match failed")

    def testTimeFromTicks(self):
        val = dbapi.TimeFromTicks(1173804319)
        self.assert_(val == datetime.time(10, 45, 19),
                "TimeFromTicks constructor value match failed")

    def testTimestampFromTicks(self):
        val = dbapi.TimestampFromTicks(1173804319)
        self.assert_(val == datetime.datetime(2007, 3, 13, 10, 45, 19),
                "TimestampFromTicks constructor value match failed")

    def testBinary(self):
        v = dbapi.Binary("\x00\x01\x02\x03\x02\x01\x00")
        self.assert_(v == "\x00\x01\x02\x03\x02\x01\x00",
                "Binary value match failed")
        self.assert_(isinstance(v, pg8000.Bytea),
                "Binary type match failed")

    def testRowCount(self):
        c1 = db2.cursor()
        c1.execute("SELECT * FROM t1")
        self.assertEquals(5, c1.rowcount)

if __name__ == "__main__":
    unittest.main()

