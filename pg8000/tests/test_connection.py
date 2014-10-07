import unittest
import pg8000
from pg8000.tests.connection_settings import db_connect
from pg8000.six import PY2, PRE_26


# Tests related to connecting to a database.
class Tests(unittest.TestCase):
    def testSocketMissing(self):
        conn_params = {
            'unix_sock': "/file-does-not-exist",
            'user': "doesn't-matter"}
        if 'use_cache' in db_connect:
            conn_params['use_cache'] = db_connect['use_cache']
        self.assertRaises(pg8000.InterfaceError, pg8000.connect, **conn_params)

    def testDatabaseMissing(self):
        data = db_connect.copy()
        data["database"] = "missing-db"
        self.assertRaises(pg8000.ProgrammingError, pg8000.connect, **data)

    def testNotify(self):

        try:
            db = pg8000.connect(**db_connect)
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
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    # This requires a line in pg_hba.conf that requires gss for the database
    # pg8000_gss

    def testGss(self):
        data = db_connect.copy()
        data["database"] = "pg8000_gss"

        # Should raise an exception saying gss isn't supported
        if PY2:
            self.assertRaises(pg8000.InterfaceError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.InterfaceError,
                "Authentication method 7 not supported by pg8000.",
                pg8000.connect, **data)

    def testSsl(self):
        data = db_connect.copy()
        data["ssl"] = True
        if PRE_26:
            self.assertRaises(pg8000.InterfaceError, pg8000.connect, **data)
        else:
            db = pg8000.connect(**data)
            db.close()

    # This requires a line in pg_hba.conf that requires 'password' for the
    # database pg8000_password

    def testPassword(self):
        data = db_connect.copy()
        data["database"] = "pg8000_password"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

if __name__ == "__main__":
    unittest.main()
