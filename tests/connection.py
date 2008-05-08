import unittest
import pg8000
from connection_settings import db_connect

# Tests related to connecting to a database.
class Tests(unittest.TestCase):
    def testSocketMissing(self):
        self.assertRaises(pg8000.InterfaceError, pg8000.Connection,
                unix_sock="/file-does-not-exist", user="doesn't-matter")

    def testDatabaseMissing(self):
        data = db_connect.copy()
        data["database"] = "missing-db"
        self.assertRaises(pg8000.ProgrammingError, pg8000.Connection, **data)

    def testNotify(self):
        db = pg8000.Connection(**db_connect)
        db.execute("LISTEN test")
        db.execute("NOTIFY test")


if __name__ == "__main__":
    unittest.main()

