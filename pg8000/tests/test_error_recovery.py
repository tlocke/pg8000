import unittest
from pg8000 import DBAPI
from contextlib import closing
from .connection_settings import db_connect
import warnings
from ..errors import DatabaseError
import datetime

db = DBAPI.connect(**db_connect)


class TestException(Exception):
    pass


class Tests(unittest.TestCase):
    def raiseException(self, value):
        raise TestException("oh noes!")

    def testPyValueFail(self):
        # Ensure that if types.py_value throws an exception, the original
        # exception is raised (TestException), and the connection is
        # still usable after the error.
        orig = db.py_types[datetime.time]
        db.py_types[datetime.time] = (orig[0], orig[1], self.raiseException)

        with closing(db.cursor()) as c:
            try:
                try:
                    c.execute("SELECT %s as f1", (datetime.time(10, 30),))
                    c.fetchall()
                    # shouldn't get here, exception should be thrown
                    self.fail()
                except TestException:
                    # should be TestException type, this is OK!
                    db.rollback()
            finally:
                db.py_types[datetime.time] = orig

            # ensure that the connection is still usable for a new query
            c.execute("VALUES ('hw3'::text)")
            self.assert_(c.fetchone()[0] == "hw3")

    def testNoDataErrorRecovery(self):
        for i in range(1, 4):
            try:
                with closing(db.cursor()) as cursor:
                    cursor.execute("DROP TABLE t1")
            except DatabaseError as e:
                # the only acceptable error is:
                self.assert_(
                    e.args[1] == b'42P01',  # table does not exist
                    "incorrect error for drop table")
                db.rollback()

    def testClosedConnection(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            my_db = DBAPI.connect(**db_connect)
            cursor = my_db.cursor()
            my_db.close()
            self.assertRaises(
                db.InterfaceError, cursor.execute, "VALUES ('hw1'::text)")

if __name__ == "__main__":
    unittest.main()
