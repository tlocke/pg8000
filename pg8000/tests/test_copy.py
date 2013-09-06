from __future__ import with_statement

import unittest
from contextlib import closing
from pg8000 import dbapi
from .connection_settings import db_connect
from io import BytesIO

db = dbapi.connect(**db_connect)


class Tests(unittest.TestCase):
    def setUp(self):
        with closing(db.cursor()) as cursor:
            try:
                cursor.execute("DROP TABLE t1")
            except dbapi.DatabaseError as e:
                # the only acceptable error is:
                self.assert_(
                    e.args[1] == b'42P01',  # table does not exist
                    "incorrect error for drop table")
                db.rollback()
            cursor.execute(
                "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
                "f2 int not null, f3 varchar(50) null)")

    def testCopyToWithTable(self):
        with closing(db.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, 1))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 2, 2))
            cursor.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 3, 3))

            stream = BytesIO()
            cursor.copy_to(stream, "t1")
            self.assert_(stream.getvalue() == b"1\t1\t1\n2\t2\t2\n3\t3\t3\n")
            self.assert_(cursor.rowcount == 3)
            db.commit()

    def testCopyToWithQuery(self):
        with closing(db.cursor()) as cursor:
            stream = BytesIO()
            cursor.copy_to(
                stream, query="COPY (SELECT 1 as One, 2 as Two) TO STDOUT "
                "WITH DELIMITER 'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two")
            self.assertEqual(stream.getvalue(), b'oneXtwo\n1XY2Y\n')
            self.assertEqual(cursor.rowcount, 1)
            db.rollback()

    def testCopyFromWithTable(self):
        with closing(db.cursor()) as cursor:
            stream = BytesIO(b"1\t1\t1\n2\t2\t2\n3\t3\t3\n")
            cursor.copy_from(stream, "t1")
            self.assert_(cursor.rowcount == 3)

            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            retval = cursor.fetchall()
            self.assert_(retval == ((1, 1, '1'), (2, 2, '2'), (3, 3, '3')))
            db.rollback()

    def testCopyFromWithQuery(self):
        with closing(db.cursor()) as cursor:
            stream = BytesIO(b"f1Xf2\n1XY1Y\n")
            cursor.copy_from(
                stream, query="COPY t1 (f1, f2) FROM STDIN WITH DELIMITER "
                "'X' CSV HEADER QUOTE AS 'Y' FORCE NOT NULL f1")
            self.assert_(cursor.rowcount == 1)

            cursor.execute("SELECT * FROM t1 ORDER BY f1")
            retval = cursor.fetchall()
            self.assert_(retval == ((1, 1, None),))
            db.commit()

    def testCopyWithoutTableOrQuery(self):
        with closing(db.cursor()) as cursor:
            stream = BytesIO()
            self.assertRaises(
                dbapi.CopyQueryOrTableRequiredError, cursor.copy_from, stream)
            self.assertRaises(
                dbapi.CopyQueryOrTableRequiredError, cursor.copy_to, stream)
            db.rollback()


if __name__ == "__main__":
    unittest.main()
