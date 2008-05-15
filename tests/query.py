import unittest
import pg8000
import threading
from connection_settings import db_connect
db = pg8000.Connection(**db_connect)

# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.
class Tests(unittest.TestCase):
    def setUp(self):
        try:
            db.execute("DROP TABLE t1")
        except pg8000.DatabaseError, e:
            # the only acceptable error is:
            self.assert_(e.args[1] == '42P01', # table does not exist
                    "incorrect error for drop table")
        db.execute("CREATE TEMPORARY TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")

    def testDatabaseError(self):
        self.assertRaises(pg8000.ProgrammingError, db.execute, "INSERT INTO t99 VALUES (1, 2, 3)")

    def testParallelQueries(self):
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

    def testInsertReturning(self):
        db.begin()
        try:
            db.execute("CREATE TABLE t2 (id serial, data text)")

            # Test INSERT ... RETURNING with one row...
            db.execute("INSERT INTO t2 (data) VALUES ($1) RETURNING id", "test1")
            row_id = db.read_dict()["id"]
            db.execute("SELECT data FROM t2 WHERE id = $1", row_id)
            self.assert_("test1" == db.read_dict()["data"])
            self.assert_(db.row_count == 1)

            # Test with multiple rows...
            db.execute("INSERT INTO t2 (data) VALUES ($1), ($2), ($3) RETURNING id", "test2", "test3", "test4")
            self.assert_(db.row_count == 3)
            ids = tuple([x[0] for x in db.iterate_tuple()])
            self.assert_(len(ids) == 3)
        finally:
            db.rollback()

    def testMultithreadedStatement(self):
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

    def testMultithreadedCursor(self):
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

    def testRowCount(self):
        expected_count = 57
        s1 = pg8000.PreparedStatement(db, "INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", int, int, str)
        for i in range(expected_count):
            s1.execute(i, i, None)

        cur = pg8000.Cursor(db)
        cur.execute("SELECT * FROM t1")

        # Check row_count without doing any reading first...
        self.assertEquals(expected_count, cur.row_count)

        # Check row_count after reading some rows, make sure it still works...
        for i in range(expected_count // 2):
            cur.read_tuple()
        self.assertEquals(expected_count, cur.row_count)

        # Restart the cursor, read a few rows, and then check row_count again...
        cur = pg8000.Cursor(db)
        cur.execute("SELECT * FROM t1")
        for i in range(expected_count // 3):
            cur.read_tuple()
        self.assertEquals(expected_count, cur.row_count)

        # Should be -1 for a command with no results
        cur.execute("DROP TABLE t1")
        self.assertEquals(-1, cur.row_count)

    def testRowCountUpdate(self):
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 1, 1, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 2, 10, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 3, 100, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 4, 1000, None)
        db.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 5, 10000, None)
        db.execute("UPDATE t1 SET f3 = $1 WHERE f2 > 101", "Hello!")
        self.assert_(db.row_count == 2)


if __name__ == "__main__":
    unittest.main()

