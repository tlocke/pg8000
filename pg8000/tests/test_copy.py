import unittest
import pg8000
from .connection_settings import db_connect
from pg8000.six import b, BytesIO
from sys import exc_info


class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)
        try:
            cursor = self.db.cursor()
            try:
                cursor = self.db.cursor()
                cursor.execute("DROP TABLE t1")
            except pg8000.DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(
                    e.args[1], b('42P01'),  # table does not exist
                    "incorrect error for drop table")
                self.db.rollback()
            cursor.execute(
                "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
                "f2 int not null, f3 varchar(50) null)")
        finally:
            cursor.close()

    def tearDown(self):
        self.db.close()

    def testCopyToWithTable(self):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, 1))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 2, 2))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 3, 3))

            stream = BytesIO()
            cursor.execute("copy t1 to stdout", stream=stream)
            self.assertEqual(
                stream.getvalue(), b("1\t1\t1\n2\t2\t2\n3\t3\t3\n"))
            self.assertEqual(cursor.rowcount, 3)
            self.db.commit()
        finally:
            cursor.close()

    def testCopyToWithQuery(self):
        try:
            cursor = self.db.cursor()
            stream = BytesIO()
            cursor.execute(
                "COPY (SELECT 1 as One, 2 as Two) TO STDOUT WITH DELIMITER "
                "'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two", stream=stream)
            self.assertEqual(stream.getvalue(), b('oneXtwo\n1XY2Y\n'))
            self.assertEqual(cursor.rowcount, 1)
            self.db.rollback()
        finally:
            cursor.close()

    def testCopyFromWithTable(self):
        try:
            cursor = self.db.cursor()
            stream = BytesIO(b("1\t1\t1\n2\t2\t2\n3\t3\t3\n"))
            cursor.execute("copy t1 from STDIN", stream=stream)
            self.assertEqual(cursor.rowcount, 3)

            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            retval = cursor.fetchall()
            self.assertEqual(retval, ([1, 1, '1'], [2, 2, '2'], [3, 3, '3']))
            self.db.rollback()
        finally:
            cursor.close()

    def testCopyFromWithQuery(self):
        try:
            cursor = self.db.cursor()
            stream = BytesIO(b("f1Xf2\n1XY1Y\n"))
            cursor.execute(
                "COPY t1 (f1, f2) FROM STDIN WITH DELIMITER 'X' CSV HEADER "
                "QUOTE AS 'Y' FORCE NOT NULL f1", stream=stream)
            self.assertEqual(cursor.rowcount, 1)

            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            retval = cursor.fetchall()
            self.assertEqual(retval, ([1, 1, None],))
            self.db.commit()
        finally:
            cursor.close()

if __name__ == "__main__":
    unittest.main()
