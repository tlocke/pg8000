import unittest
import pg8000
from connection_settings import db_connect
from six import PY2, u
import sys
from distutils.version import LooseVersion
import socket
import struct


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

        with pg8000.connect(**db_connect) as db:
            self.assertEqual(list(db.notifications), [])
            with db.cursor() as cursor:
                cursor.execute("LISTEN test")
                cursor.execute("NOTIFY test")
                db.commit()

                cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
                self.assertEqual(len(db.notifications), 1)
                self.assertEqual(db.notifications[0][1], "test")

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
        with pg8000.connect(**db_connect) as db:
            # Create user
            username = 'boltzmann'
            password = u('cha\uFF6Fs')
            cur = db.cursor()

            # Delete user if left over from previous run
            try:
                cur.execute("drop role " + username)
            except pg8000.ProgrammingError:
                cur.execute("rollback")

            cur.execute(
                "create user " + username + " with password '" + password +
                "';")
            cur.execute('commit;')

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

        with pg8000.connect(**db_connect) as db:
            cur = db.cursor()
            cur.execute("drop role " + username)
            cur.execute("commit;")

    def testBrokenPipe(self):
        with pg8000.connect(**db_connect) as db1:
            with pg8000.connect(**db_connect) as db2:
                with db1.cursor() as cur1, db2.cursor() as cur2:

                    cur1.execute("select pg_backend_pid()")
                    pid1 = cur1.fetchone()[0]

                    cur2.execute(
                            "select pg_terminate_backend(%s)", (pid1,))
                    try:
                        cur1.execute("select 1")
                    except Exception as e:
                        self.assertTrue(
                            isinstance(e, (socket.error, struct.error)))

    def testApplicatioName(self):
        params = db_connect.copy()
        params['application_name'] = 'my test application name'
        db = pg8000.connect(**params)
        cur = db.cursor()

        if db._server_version >= LooseVersion('9.2'):
            cur.execute('select application_name from pg_stat_activity '
                        ' where pid = pg_backend_pid()')
        else:
            # for pg9.1 and earlier, procpod field rather than pid
            cur.execute('select application_name from pg_stat_activity '
                        ' where procpid = pg_backend_pid()')

        application_name = cur.fetchone()[0]
        self.assertEqual(application_name, 'my test application name')


if __name__ == "__main__":
    unittest.main()
