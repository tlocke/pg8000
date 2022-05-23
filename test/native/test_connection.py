import struct

from datetime import time as Time

import pytest

from pg8000.native import Connection, DatabaseError, InterfaceError, __version__


def test_unix_socket_missing():
    conn_params = {"unix_sock": "/file-does-not-exist", "user": "doesn't-matter"}

    with pytest.raises(InterfaceError):
        Connection(**conn_params)


def test_internet_socket_connection_refused():
    conn_params = {"port": 0, "user": "doesn't-matter"}

    with pytest.raises(
        InterfaceError,
        match="Can't create a connection to host localhost and port 0 "
        "\\(timeout is None and source_address is None\\).",
    ):
        Connection(**conn_params)


def test_database_missing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(DatabaseError):
        Connection(**db_kwargs)


def test_notify(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test")

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "")


def test_notify_with_payload(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test, 'Parnham'")

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "Parnham")


# This requires a line in pg_hba.conf that requires md5 for the database
# pg8000_md5


def test_md5(db_kwargs):
    db_kwargs["database"] = "pg8000_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        Connection(**db_kwargs)


# This requires a line in pg_hba.conf that requires 'password' for the
# database pg8000_password


def test_password(db_kwargs):
    db_kwargs["database"] = "pg8000_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        Connection(**db_kwargs)


def test_unicode_databaseName(db_kwargs):
    db_kwargs["database"] = "pg8000_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        Connection(**db_kwargs)


def test_bytes_databaseName(db_kwargs):
    """Should only raise an exception saying db doesn't exist"""

    db_kwargs["database"] = bytes("pg8000_sn\uFF6Fw", "utf8")
    with pytest.raises(DatabaseError, match="3D000"):
        Connection(**db_kwargs)


def test_bytes_password(con, db_kwargs):
    # Create user
    username = "boltzmann"
    password = "cha\uFF6Fs"
    con.run("create user " + username + " with password '" + password + "';")

    db_kwargs["user"] = username
    db_kwargs["password"] = password.encode("utf8")
    db_kwargs["database"] = "pg8000_md5"
    with pytest.raises(DatabaseError, match="3D000"):
        Connection(**db_kwargs)

    con.run("drop role " + username)


def test_broken_pipe_read(con, db_kwargs):
    db1 = Connection(**db_kwargs)
    res = db1.run("select pg_backend_pid()")
    pid1 = res[0][0]

    con.run("select pg_terminate_backend(:v)", v=pid1)
    with pytest.raises(InterfaceError, match="network error"):
        db1.run("select 1")

    try:
        db1.close()
    except InterfaceError:
        pass


def test_broken_pipe_unpack(con):
    res = con.run("select pg_backend_pid()")
    pid1 = res[0][0]

    with pytest.raises(InterfaceError, match="network error"):
        con.run("select pg_terminate_backend(:v)", v=pid1)


def test_broken_pipe_flush(con, db_kwargs):
    db1 = Connection(**db_kwargs)
    res = db1.run("select pg_backend_pid()")
    pid1 = res[0][0]

    con.run("select pg_terminate_backend(:v)", v=pid1)
    try:
        db1.run("select 1")
    except BaseException:
        pass

    # Sometimes raises and sometime doesn't
    try:
        db1.close()
    except InterfaceError as e:
        assert str(e) == "network error"


def test_application_name(db_kwargs):
    app_name = "my test application name"
    db_kwargs["application_name"] = app_name
    with Connection(**db_kwargs) as db:
        res = db.run(
            "select application_name from pg_stat_activity "
            " where pid = pg_backend_pid()"
        )

        application_name = res[0][0]
        assert application_name == app_name


def test_application_name_integer(db_kwargs):
    db_kwargs["application_name"] = 1
    with pytest.raises(
        InterfaceError,
        match="The parameter application_name can't be of type <class 'int'>.",
    ):
        Connection(**db_kwargs)


def test_application_name_bytearray(db_kwargs):
    db_kwargs["application_name"] = bytearray(b"Philby")
    with Connection(**db_kwargs):
        pass


class PG8000TestException(Exception):
    pass


def raise_exception(val):
    raise PG8000TestException("oh noes!")


def test_py_value_fail(con, mocker):
    # Ensure that if types.py_value throws an exception, the original
    # exception is raised (PG8000TestException), and the connection is
    # still usable after the error.
    mocker.patch.object(con, "py_types")
    con.py_types = {Time: raise_exception}

    with pytest.raises(PG8000TestException):
        con.run("SELECT CAST(:v AS TIME)", v=Time(10, 30))

        # ensure that the connection is still usable for a new query
        res = con.run("VALUES ('hw3'::text)")
        assert res[0][0] == "hw3"


def test_no_data_error_recovery(con):
    for i in range(1, 4):
        with pytest.raises(DatabaseError) as e:
            con.run("DROP TABLE t1")
        assert e.value.args[0]["C"] == "42P01"
        con.run("ROLLBACK")


def test_closed_connection(con):
    con.close()
    with pytest.raises(InterfaceError, match="connection is closed"):
        con.run("VALUES ('hw1'::text)")


def test_network_error_on_connect(db_kwargs, mocker):
    mocker.patch("pg8000.core.ci_unpack", side_effect=struct.error())
    with pytest.raises(InterfaceError, match="network error"):
        Connection(**db_kwargs)


def test_version():
    try:
        from importlib.metadata import version
    except ImportError:
        from importlib_metadata import version

    v = version("pg8000")

    assert __version__ == v


@pytest.mark.parametrize(
    "commit",
    [
        "commit",
        "COMMIT;",
    ],
)
def test_failed_transaction_commit(con, commit):
    con.run("create temporary table tt (f1 int primary key)")
    con.run("begin")
    try:
        con.run("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    with pytest.raises(InterfaceError):
        con.run(commit)


@pytest.mark.parametrize(
    "rollback",
    [
        "rollback",
        "rollback;",
        "ROLLBACK ;",
    ],
)
def test_failed_transaction_rollback(con, rollback):
    con.run("create temporary table tt (f1 int primary key)")
    con.run("begin")
    try:
        con.run("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    con.run(rollback)


@pytest.mark.parametrize(
    "rollback",
    [
        "rollback to sp",
        "rollback to sp;",
        "ROLLBACK TO sp ;",
    ],
)
def test_failed_transaction_rollback_to_savepoint(con, rollback):
    con.run("create temporary table tt (f1 int primary key)")
    con.run("begin")
    con.run("SAVEPOINT sp;")

    try:
        con.run("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    con.run(rollback)


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN",
        "select * from tt;",
    ],
)
def test_failed_transaction_sql(con, sql):
    con.run("create temporary table tt (f1 int primary key)")
    con.run("begin")
    try:
        con.run("insert into tt(f1) values(null)")
    except DatabaseError:
        pass

    with pytest.raises(DatabaseError):
        con.run(sql)
