import unittest
from pg8000 import DBAPI
from .connection_settings import db_connect
import warnings
from pg8000.errors import DatabaseError
import datetime
from sys import exc_info
from pg8000.six import b

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

        try:
            c = db.cursor()
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
            self.assertEqual(c.fetchone()[0], "hw3")
        finally:
            c.close()

    def testNoDataErrorRecovery(self):
        for i in range(1, 4):
            try:
                try:
                    cursor = db.cursor()
                    cursor.execute("DROP TABLE t1")
                finally:
                    cursor.close()
            except DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(e.args[1], b('42P01'))  # table does not exist
                db.rollback()

    def testClosedConnection(self):
        warnings.simplefilter("ignore")
        my_db = DBAPI.connect(**db_connect)
        cursor = my_db.cursor()
        my_db.close()
        self.assertRaises(
            db.InterfaceError, cursor.execute, "VALUES ('hw1'::text)")
        warnings.resetwarnings()

if __name__ == "__main__":
    unittest.main()
