import datetime
import socket
import warnings

import pytest

from pg8000.dbapi import DatabaseError, InterfaceError, __version__, connect


def test_unix_socket_missing():
    conn_params = {"unix_sock": "/file-does-not-exist", "user": "doesn't-matter"}

    with pytest.raises(InterfaceError):
        with connect(**conn_params) as con:
            con.close()


def test_internet_socket_connection_refused():
    conn_params = {"port": 0, "user": "doesn't-matter"}

    with pytest.raises(
        InterfaceError,
        match="Can't create a connection to host localhost and port 0 "
        "\\(timeout is None and source_address is None\\).",
    ):
        connect(**conn_params)


def test_Connection_plain_socket(db_kwargs):
    host = db_kwargs.get("host", "localhost")
    port = db_kwargs.get("port", 5432)
    with socket.create_connection((host, port)) as sock:
        user = db_kwargs["user"]
        password = db_kwargs["password"]
        conn_params = {"sock": sock, "user": user, "password": password}

        con = connect(**conn_params)
        cur = con.cursor()

        cur.execute("SELECT 1")
        res = cur.fetchall()
        assert res[0][0] == 1


def test_database_missing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(DatabaseError):
        connect(**db_kwargs)


def test_database_name_unicode(db_kwargs):
    db_kwargs["database"] = "pg8000_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        connect(**db_kwargs)


def test_database_name_bytes(db_kwargs):
    """Should only raise an exception saying db doesn't exist"""

    db_kwargs["database"] = bytes("pg8000_sn\uFF6Fw", "utf8")
    with pytest.raises(DatabaseError, match="3D000"):
        connect(**db_kwargs)


def test_password_bytes(con, db_kwargs):
    # Create user
    username = "boltzmann"
    password = "cha\uFF6Fs"
    cur = con.cursor()
    cur.execute("create user " + username + " with password '" + password + "';")
    con.commit()

    db_kwargs["user"] = username
    db_kwargs["password"] = password.encode("utf8")
    db_kwargs["database"] = "pg8000_md5"
    with pytest.raises(DatabaseError, match="3D000"):
        connect(**db_kwargs)

    cur.execute("drop role " + username)
    con.commit()


def test_application_name(db_kwargs):
    app_name = "my test application name"
    db_kwargs["application_name"] = app_name
    with connect(**db_kwargs) as db:
        cur = db.cursor()
        cur.execute(
            "select application_name from pg_stat_activity "
            " where pid = pg_backend_pid()"
        )

        application_name = cur.fetchone()[0]
        assert application_name == app_name


def test_application_name_integer(db_kwargs):
    db_kwargs["application_name"] = 1
    with pytest.raises(
        InterfaceError,
        match="The parameter application_name can't be of type " "<class 'int'>.",
    ):
        connect(**db_kwargs)


def test_application_name_bytearray(db_kwargs):
    db_kwargs["application_name"] = bytearray(b"Philby")
    with connect(**db_kwargs):
        pass


def test_notify(con):
    cursor = con.cursor()
    cursor.execute("select pg_backend_pid()")
    backend_pid = cursor.fetchall()[0][0]
    assert list(con.notifications) == []
    cursor.execute("LISTEN test")
    cursor.execute("NOTIFY test")
    con.commit()

    cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "")


def test_notify_with_payload(con):
    cursor = con.cursor()
    cursor.execute("select pg_backend_pid()")
    backend_pid = cursor.fetchall()[0][0]
    assert list(con.notifications) == []
    cursor.execute("LISTEN test")
    cursor.execute("NOTIFY test, 'Parnham'")
    con.commit()

    cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "Parnham")


def test_broken_pipe_read(con, db_kwargs):
    db1 = connect(**db_kwargs)
    cur1 = db1.cursor()
    cur2 = con.cursor()
    cur1.execute("select pg_backend_pid()")
    pid1 = cur1.fetchone()[0]

    cur2.execute("select pg_terminate_backend(%s)", (pid1,))
    with pytest.raises(InterfaceError, match="network error"):
        cur1.execute("select 1")

    try:
        db1.close()
    except InterfaceError:
        pass


def test_broken_pipe_flush(con, db_kwargs):
    db1 = connect(**db_kwargs)
    cur1 = db1.cursor()
    cur2 = con.cursor()
    cur1.execute("select pg_backend_pid()")
    pid1 = cur1.fetchone()[0]

    cur2.execute("select pg_terminate_backend(%s)", (pid1,))
    try:
        cur1.execute("select 1")
    except BaseException:
        pass

    # Sometimes raises and sometime doesn't
    try:
        db1.close()
    except InterfaceError as e:
        assert str(e) == "network error"


def test_broken_pipe_unpack(con):
    cur = con.cursor()
    cur.execute("select pg_backend_pid()")
    pid1 = cur.fetchone()[0]

    with pytest.raises(InterfaceError, match="network error"):
        cur.execute("select pg_terminate_backend(%s)", (pid1,))


def test_py_value_fail(con, mocker):
    # Ensure that if types.py_value throws an exception, the original
    # exception is raised (PG8000TestException), and the connection is
    # still usable after the error.

    class PG8000TestException(Exception):
        pass

    def raise_exception(val):
        raise PG8000TestException("oh noes!")

    mocker.patch.object(con, "py_types")
    con.py_types = {datetime.time: raise_exception}

    with pytest.raises(PG8000TestException):
        c = con.cursor()
        c.execute("SELECT CAST(%s AS TIME) AS f1", (datetime.time(10, 30),))
        c.fetchall()

        # ensure that the connection is still usable for a new query
        c.execute("VALUES ('hw3'::text)")
        assert c.fetchone()[0] == "hw3"


def test_no_data_error_recovery(con):
    for i in range(1, 4):
        with pytest.raises(DatabaseError) as e:
            c = con.cursor()
            c.execute("DROP TABLE t1")
        assert e.value.args[0]["C"] == "42P01"
        con.rollback()


def test_closed_connection(db_kwargs):
    warnings.simplefilter("ignore")

    my_db = connect(**db_kwargs)
    cursor = my_db.cursor()
    my_db.close()
    with pytest.raises(my_db.InterfaceError, match="connection is closed"):
        cursor.execute("VALUES ('hw1'::text)")

    warnings.resetwarnings()


def test_version():
    try:
        from importlib.metadata import version
    except ImportError:
        from importlib_metadata import version

    ver = version("pg8000")

    assert __version__ == ver


@pytest.mark.parametrize(
    "commit",
    [
        "commit",
        "COMMIT;",
    ],
)
def test_failed_transaction_commit_sql(cursor, commit):
    cursor.execute("create temporary table tt (f1 int primary key)")
    cursor.execute("begin")
    try:
        cursor.execute("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    with pytest.raises(InterfaceError):
        cursor.execute(commit)


def test_failed_transaction_commit_method(con, cursor):
    cursor.execute("create temporary table tt (f1 int primary key)")
    cursor.execute("begin")
    try:
        cursor.execute("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    with pytest.raises(InterfaceError):
        con.commit()


@pytest.mark.parametrize(
    "rollback",
    [
        "rollback",
        "rollback;",
        "ROLLBACK ;",
    ],
)
def test_failed_transaction_rollback_sql(cursor, rollback):
    cursor.execute("create temporary table tt (f1 int primary key)")
    cursor.execute("begin")
    try:
        cursor.execute("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    cursor.execute(rollback)


def test_failed_transaction_rollback_method(cursor, con):
    cursor.execute("create temporary table tt (f1 int primary key)")
    cursor.execute("begin")
    try:
        cursor.execute("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    con.rollback()


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN",
        "select * from tt;",
    ],
)
def test_failed_transaction_sql(cursor, sql):
    cursor.execute("create temporary table tt (f1 int primary key)")
    cursor.execute("begin")
    try:
        cursor.execute("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    with pytest.raises(DatabaseError):
        cursor.execute(sql)
