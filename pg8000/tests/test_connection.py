import unittest
import pg8000
from pg8000.tests.connection_settings import db_connect
from six import PY2, u
import sys


# Check if running in Jython
if 'java' in sys.platform:
    from javax.net.ssl import TrustManager, X509TrustManager
    from jarray import array
    from javax.net.ssl import SSLContext

    class TrustAllX509TrustManager(X509TrustManager):
        '''Define a custom TrustManager which will blindly accept all
        certificates'''

        def checkClientTrusted(self, chain, auth):
            pass

        def checkServerTrusted(self, chain, auth):
            pass

        def getAcceptedIssuers(self):
            return None
    # Create a static reference to an SSLContext which will use
    # our custom TrustManager
    trust_managers = array([TrustAllX509TrustManager()], TrustManager)
    TRUST_ALL_CONTEXT = SSLContext.getInstance("SSL")
    TRUST_ALL_CONTEXT.init(None, trust_managers, None)
    # Keep a static reference to the JVM's default SSLContext for restoring
    # at a later time
    DEFAULT_CONTEXT = SSLContext.getDefault()


def trust_all_certificates(f):
    '''Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates'''
    def wrapped(*args, **kwargs):
        # Only do this if running under Jython
        if 'java' in sys.platform:
            from javax.net.ssl import SSLContext
            SSLContext.setDefault(TRUST_ALL_CONTEXT)
            try:
                res = f(*args, **kwargs)
                return res
            finally:
                SSLContext.setDefault(DEFAULT_CONTEXT)
        else:
            return f(*args, **kwargs)
    return wrapped


# Tests related to connecting to a database.
class Tests(unittest.TestCase):
    def testSocketMissing(self):
        conn_params = {
            'unix_sock': "/file-does-not-exist",
            'user': "doesn't-matter"}
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

    @trust_all_certificates
    def testSsl(self):
        data = db_connect.copy()
        data["ssl"] = True
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

    def testUnicodeDatabaseName(self):
        data = db_connect.copy()
        data["database"] = "pg8000_sn\uFF6Fw"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    def testBytesDatabaseName(self):
        data = db_connect.copy()

        # Should only raise an exception saying db doesn't exist
        if PY2:
            data["database"] = "pg8000_sn\uFF6Fw"
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            data["database"] = bytes("pg8000_sn\uFF6Fw", 'utf8')
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    def testBytesPassword(self):
        db = pg8000.connect(**db_connect)
        # Create user
        username = 'boltzmann'
        password = u('cha\uFF6Fs')
        cur = db.cursor()
        cur.execute(
            "create user " + username + " with password '" + password + "';")
        cur.execute('commit;')
        db.close()

        data = db_connect.copy()
        data['user'] = username
        data['password'] = password.encode('utf8')
        data['database'] = 'pg8000_md5'
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

        db = pg8000.connect(**db_connect)
        cur = db.cursor()
        cur.execute("drop role " + username)
        cur.execute("commit;")
        db.close()
        '''
        data = db_connect.copy()

        # Should only raise an exception saying db doesn't exist
        if PY2:
            data["database"] = "pg8000_sn\uFF6Fw"
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            data["database"] = bytes("pg8000_sn\uFF6Fw", 'utf8')
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)
        '''

    def testBrokenPipe(self):
        db1 = pg8000.connect(**db_connect)
        db2 = pg8000.connect(**db_connect)

        cur1 = db1.cursor()
        cur2 = db2.cursor()

        cur1.execute("select pg_backend_pid()")
        pid1 = cur1.fetchone()[0]

        cur2.execute("select pg_terminate_backend(%s)", (pid1,))
        self.assertRaises(pg8000.OperationalError, cur1.execute, "select 1")
        cur2.close()
        db2.close()

if __name__ == "__main__":
    unittest.main()
