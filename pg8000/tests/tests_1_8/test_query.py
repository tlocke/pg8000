import unittest
import threading
from pg8000 import dbapi
from .connection_settings import db_connect
from pg8000.six import u, b
from sys import exc_info


from warnings import filterwarnings


# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.
class Tests(unittest.TestCase):
    def setUp(self):
        self.db = dbapi.connect(**db_connect)
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")
        self.db.paramstyle = 'format'
        try:
            cursor = self.db.cursor()
            try:
                cursor.execute("DROP TABLE t1")
            except dbapi.DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(e.args[1], b('42P01'))  # table does not exist
                self.db.rollback()
            cursor.execute(
                "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
                "f2 bigint not null, f3 varchar(50) null)")
        finally:
            cursor.close()

        self.db.commit()

    def tearDown(self):
        self.db.close()

    def testDatabaseError(self):
        try:
            cursor = self.db.cursor()
            self.assertRaises(
                dbapi.ProgrammingError, cursor.execute,
                "INSERT INTO t99 VALUES (1, 2, 3)")
        finally:
            cursor.close()

        self.db.rollback()

    def testParallelQueries(self):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (1, 1, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (2, 10, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (3, 100, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (4, 1000, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (5, 10000, None))
            try:
                c1 = self.db.cursor()
                c2 = self.db.cursor()
                c1.execute("SELECT f1, f2, f3 FROM t1")
                for row in c1:
                    f1, f2, f3 = row
                    c2.execute(
                        "SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
                    for row in c2:
                        f1, f2, f3 = row
            finally:
                c1.close()
                c2.close()
        finally:
            cursor.close()
        self.db.rollback()

    def testInsertReturning(self):
        try:
            cursor = self.db.cursor()
            cursor.execute("CREATE TABLE t2 (id serial, data text)")

            # Test INSERT ... RETURNING with one row...
            cursor.execute(
                "INSERT INTO t2 (data) VALUES (%s) RETURNING id",
                ("test1",))
            row_id = cursor.fetchone()[0]
            cursor.execute("SELECT data FROM t2 WHERE id = %s", (row_id,))
            self.assertEqual("test1", cursor.fetchone()[0])

            # In PostgreSQL 8.4 we don't know the row count for a select
            if not self.db._server_version.startswith("8.4"):
                self.assertEqual(cursor.rowcount, 1)

            # Test with multiple rows...
            cursor.execute(
                "INSERT INTO t2 (data) VALUES (%s), (%s), (%s) "
                "RETURNING id", ("test2", "test3", "test4"))

            # In PostgreSQL 8.4 we don't know the row count for a select
            if not self.db._server_version.startswith("8.4"):
                self.assertEqual(cursor.rowcount, 3)

            ids = tuple([x[0] for x in cursor])
            self.assertEqual(len(ids), 3)
        finally:
            cursor.close()
            self.db.rollback()

    def testMultithreadedCursor(self):
        try:
            cursor = self.db.cursor()
            # Note: Multithreading with a cursor is not highly recommended due
            # to low performance.

            def test(left, right):
                for i in range(left, right):
                    cursor.execute(
                        "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                        (i, id(threading.currentThread()), None))
            t1 = threading.Thread(target=test, args=(1, 25))
            t2 = threading.Thread(target=test, args=(25, 50))
            t3 = threading.Thread(target=test, args=(50, 75))
            t1.start()
            t2.start()
            t3.start()
            t1.join()
            t2.join()
            t3.join()
        finally:
            cursor.close()
            self.db.rollback()

    def testRowCount(self):
        # In PostgreSQL 8.4 we don't know the row count for a select
        if not self.db._server_version.startswith("8.4"):
            try:
                cursor = self.db.cursor()
                expected_count = 57
                cursor.executemany(
                    "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                    tuple((i, i, None) for i in range(expected_count)))
                self.db.commit()

                cursor.execute("SELECT * FROM t1")

                # Check row_count without doing any reading first...
                self.assertEqual(expected_count, cursor.rowcount)

                # Check rowcount after reading some rows, make sure it still
                # works...
                for i in range(expected_count // 2):
                    cursor.fetchone()
                self.assertEqual(expected_count, cursor.rowcount)
            finally:
                cursor.close()
                self.db.commit()

            try:
                cursor = self.db.cursor()
                # Restart the cursor, read a few rows, and then check rowcount
                # again...
                cursor = self.db.cursor()
                cursor.execute("SELECT * FROM t1")
                for i in range(expected_count // 3):
                    cursor.fetchone()
                self.assertEqual(expected_count, cursor.rowcount)
                self.db.rollback()

                # Should be -1 for a command with no results
                cursor.execute("DROP TABLE t1")
                self.assertEqual(-1, cursor.rowcount)
            finally:
                cursor.close()
                self.db.commit()

    def testRowCountUpdate(self):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (1, 1, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (2, 10, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (3, 100, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (4, 1000, None))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (5, 10000, None))
            cursor.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
            self.assertEqual(cursor.rowcount, 2)
        finally:
            cursor.close()
            self.db.commit()

    def testIntOid(self):
        try:
            cursor = self.db.cursor()
            # https://bugs.launchpad.net/pg8000/+bug/230796
            cursor.execute(
                "SELECT typname FROM pg_type WHERE oid = %s", (100,))
        finally:
            cursor.close()
            self.db.rollback()

    def testUnicodeQuery(self):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                u(
                    "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
                    "(\u0438\u043c\u044f VARCHAR(50), "
                    "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))"))
        finally:
            cursor.close()
            self.db.commit()


if __name__ == "__main__":
    unittest.main()
