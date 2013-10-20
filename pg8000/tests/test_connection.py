from __future__ import with_statement

import unittest
from pg8000 import dbapi
from contextlib import closing
from .connection_settings import db_connect


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
        with closing(dbapi.connect(**db_connect)) as db:
            self.assertEquals(db.notifies, [])

            with closing(db.cursor()) as cursor:
                cursor.execute("LISTEN test")
                cursor.execute("NOTIFY test")
                db.commit()

                cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
                self.assertEquals(len(db.notifies), 1)
                self.assertEquals(db.notifies[0][1], "test")

    # This requires a line in pg_hba.conf that requires md5 for the database
    # pg8000_md5

    def testMd5(self):
        data = db_connect.copy()
        data["database"] = "pg8000_md5"

        # Should only raise an exception saying db doesn't exist
        self.assertRaisesRegex(
            dbapi.ProgrammingError, '3D000', dbapi.connect, **data)

    def testSsl(self):
        data = db_connect.copy()
        data["ssl"] = True
        with closing(dbapi.connect(**data)):
            pass

if __name__ == "__main__":
    unittest.main()
