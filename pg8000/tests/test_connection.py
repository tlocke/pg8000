import unittest
from pg8000 import dbapi
from .connection_settings import db_connect
from pg8000.six import PY2, PRE_26


# Tests related to connecting to a database.
class Tests(unittest.TestCase):
    def testSocketMissing(self):
        self.assertRaises(
            dbapi.InterfaceError, dbapi.connect,
            unix_sock="/file-does-not-exist", user="doesn't-matter")

    def testDatabaseMissing(self):
        data = db_connect.copy()
        data["database"] = "missing-db"
        self.assertRaises(dbapi.ProgrammingError, dbapi.connect, **data)

    def testNotify(self):

        try:
            db = dbapi.connect(**db_connect)
            self.assertEqual(db.notifies, [])
            cursor = db.cursor()
            cursor.execute("LISTEN test")
            cursor.execute("NOTIFY test")
            db.commit()

            cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
            self.assertEqual(len(db.notifies), 1)
            self.assertEqual(db.notifies[0][1], "test")
        finally:
            cursor.close()
            db.close()

    # This requires a line in pg_hba.conf that requires md5 for the database
    # pg8000_md5

    def testMd5(self):
        data = db_connect.copy()
        data["database"] = "pg8000_md5"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                dbapi.ProgrammingError, dbapi.connect, **data)
        else:
            self.assertRaisesRegex(
                dbapi.ProgrammingError, '3D000', dbapi.connect, **data)

    def testSsl(self):
        data = db_connect.copy()
        data["ssl"] = True
        if PRE_26:
            self.assertRaises(dbapi.InterfaceError, dbapi.connect, **data)
        else:
            db = dbapi.connect(**data)
            db.close()

if __name__ == "__main__":
    unittest.main()
