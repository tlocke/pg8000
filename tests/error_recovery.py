import unittest
from pg8000 import DBAPI, DatabaseError
from .connection_settings import db_connect

db = DBAPI.connect(**db_connect)

class TestException(Exception):
    pass

class Tests(unittest.TestCase):
    def raiseException(self, *args, **kwargs):
        raise TestException("oh noes!")

    def testPyValueFail(self):
        # Ensure that if types.py_value throws an exception, the original
        # exception is raised (TestException), and the connection is
        # still usable after the error.
        from pg8000 import types
        original_py_value = types.py_value
        types.py_value = self.raiseException
        try:
            c = db.cursor()
            try:
                c.execute("VALUES ('hw1'::text), ('hw2'::text)")
                c.fetchall()
                # shouldn't get here, exception should be thrown
                self.fail()
            except TestException:
                # should be TestException type, this is OK!
                pass
        finally:
            types.py_value = original_py_value

        # ensure that the connection is still usable for a new query
        c.execute("VALUES ('hw3'::text)")
        self.assert_(c.fetchone()[0] == "hw3")

    def testNoDataErrorRecovery(self):
        for i in range(1, 4):
            try:
                db.cursor().execute("DROP TABLE t1")
            except DatabaseError as e:
                # the only acceptable error is:
                self.assert_(e.args[1] == b'42P01', # table does not exist
                        "incorrect error for drop table")

    def testClosedConnection(self):
        my_db = DBAPI.connect(**db_connect)
        cursor = my_db.cursor()
        my_db.close()
        self.assertRaises(db.InterfaceError, cursor.execute, "VALUES ('hw1'::text)")

if __name__ == "__main__":
    unittest.main()
