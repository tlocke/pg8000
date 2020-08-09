import socket
import ssl
import struct
import sys

import pg8000

import pytest


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


@pytest.fixture
def trust_all_certificates(request):
    '''Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates'''
    # Only do this if running under Jython
    is_java = 'java' in sys.platform

    if is_java:
        from javax.net.ssl import SSLContext
        SSLContext.setDefault(TRUST_ALL_CONTEXT)

    def fin():
        if is_java:
            SSLContext.setDefault(DEFAULT_CONTEXT)

    request.addfinalizer(fin)


def testUnixSocketMissing():
    conn_params = {
        'unix_sock': "/file-does-not-exist",
        'user': "doesn't-matter"
    }

    with pytest.raises(pg8000.InterfaceError):
        pg8000.connect(**conn_params)


def test_internet_socket_connection_refused():
    conn_params = {
        'port': 0,
        'user': "doesn't-matter"
    }

    with pytest.raises(
            pg8000.InterfaceError,
            match="Can't create a connection to host localhost and port 0 "
            "\\(timeout is None and source_address is None\\)."):
        pg8000.connect(**conn_params)


def testDatabaseMissing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(pg8000.ProgrammingError):
        pg8000.connect(**db_kwargs)


def test_notify(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test")
    con.commit()

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", '')


def test_notify_with_payload(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test, 'Parnham'")
    con.commit()

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", 'Parnham')


# This requires a line in pg_hba.conf that requires md5 for the database
# pg8000_md5

def testMd5(db_kwargs):
    db_kwargs["database"] = "pg8000_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.ProgrammingError, match='3D000'):
        pg8000.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires gss for the database
# pg8000_gss

def testGss(db_kwargs):
    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
            pg8000.InterfaceError,
            match="Authentication method 7 not supported by pg8000."):
        pg8000.connect(**db_kwargs)


@pytest.mark.usefixtures("trust_all_certificates")
def testSsl(db_kwargs):
    context = ssl.SSLContext()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    with pg8000.connect(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires 'password' for the
# database pg8000_password

def testPassword(db_kwargs):
    db_kwargs["database"] = "pg8000_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.ProgrammingError, match='3D000'):
        pg8000.connect(**db_kwargs)


def testUnicodeDatabaseName(db_kwargs):
    db_kwargs["database"] = "pg8000_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.ProgrammingError, match='3D000'):
        pg8000.connect(**db_kwargs)


def testBytesDatabaseName(db_kwargs):
    """ Should only raise an exception saying db doesn't exist """

    db_kwargs["database"] = bytes("pg8000_sn\uFF6Fw", 'utf8')
    with pytest.raises(pg8000.ProgrammingError, match='3D000'):
        pg8000.connect(**db_kwargs)


def testBytesPassword(con, db_kwargs):
    # Create user
    username = 'boltzmann'
    password = 'cha\uFF6Fs'
    with con.cursor() as cur:
        cur.execute(
            "create user " + username + " with password '" + password + "';")
        con.commit()

        db_kwargs['user'] = username
        db_kwargs['password'] = password.encode('utf8')
        db_kwargs['database'] = 'pg8000_md5'
        with pytest.raises(pg8000.ProgrammingError, match='3D000'):
            pg8000.connect(**db_kwargs)

        cur.execute("drop role " + username)
        con.commit()


def test_broken_pipe(con, db_kwargs):
    with pg8000.connect(**db_kwargs) as db1:
        with db1.cursor() as cur1, con.cursor() as cur2:
            cur1.execute("select pg_backend_pid()")
            pid1 = cur1.fetchone()[0]

            cur2.execute("select pg_terminate_backend(%s)", (pid1,))
            try:
                cur1.execute("select 1")
            except Exception as e:
                assert isinstance(e, (socket.error, struct.error))


def testApplicatioName(db_kwargs):
    app_name = 'my test application name'
    db_kwargs['application_name'] = app_name
    with pg8000.connect(**db_kwargs) as db:
        cur = db.cursor()
        cur.execute(
            'select application_name from pg_stat_activity '
            ' where pid = pg_backend_pid()')

        application_name = cur.fetchone()[0]
        assert application_name == app_name


def test_application_name_integer(db_kwargs):
    db_kwargs['application_name'] = 1
    with pytest.raises(
            pg8000.InterfaceError,
            match="The parameter application_name can't be of type "
            "<class 'int'>."):
        pg8000.connect(**db_kwargs)


def test_application_name_bytearray(db_kwargs):
    db_kwargs['application_name'] = bytearray(b'Philby')
    pg8000.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256

def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.ProgrammingError, match='3D000'):
        pg8000.connect(**db_kwargs)
