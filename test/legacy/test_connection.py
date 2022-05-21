import pytest

from pg8000 import DatabaseError, InterfaceError, ProgrammingError, connect


def testUnixSocketMissing():
    conn_params = {"unix_sock": "/file-does-not-exist", "user": "doesn't-matter"}

    with pytest.raises(InterfaceError):
        connect(**conn_params)


def test_internet_socket_connection_refused():
    conn_params = {"port": 0, "user": "doesn't-matter"}

    with pytest.raises(
        InterfaceError,
        match="Can't create a connection to host localhost and port 0 "
        "\\(timeout is None and source_address is None\\).",
    ):
        connect(**conn_params)


def testDatabaseMissing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(ProgrammingError):
        connect(**db_kwargs)


def test_notify(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test")
    con.commit()

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "")


def test_notify_with_payload(con):
    backend_pid = con.run("select pg_backend_pid()")[0][0]
    assert list(con.notifications) == []
    con.run("LISTEN test")
    con.run("NOTIFY test, 'Parnham'")
    con.commit()

    con.run("VALUES (1, 2), (3, 4), (5, 6)")
    assert len(con.notifications) == 1
    assert con.notifications[0] == (backend_pid, "test", "Parnham")


# This requires a line in pg_hba.conf that requires md5 for the database
# pg8000_md5


def testMd5(db_kwargs):
    db_kwargs["database"] = "pg8000_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(ProgrammingError, match="3D000"):
        connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires 'password' for the
# database pg8000_password


def testPassword(db_kwargs):
    db_kwargs["database"] = "pg8000_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(ProgrammingError, match="3D000"):
        connect(**db_kwargs)


def testUnicodeDatabaseName(db_kwargs):
    db_kwargs["database"] = "pg8000_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(ProgrammingError, match="3D000"):
        connect(**db_kwargs)


def testBytesDatabaseName(db_kwargs):
    """Should only raise an exception saying db doesn't exist"""

    db_kwargs["database"] = bytes("pg8000_sn\uFF6Fw", "utf8")
    with pytest.raises(ProgrammingError, match="3D000"):
        connect(**db_kwargs)


def testBytesPassword(con, db_kwargs):
    # Create user
    username = "boltzmann"
    password = "cha\uFF6Fs"
    with con.cursor() as cur:
        cur.execute("create user " + username + " with password '" + password + "';")
        con.commit()

        db_kwargs["user"] = username
        db_kwargs["password"] = password.encode("utf8")
        db_kwargs["database"] = "pg8000_md5"
        with pytest.raises(ProgrammingError, match="3D000"):
            connect(**db_kwargs)

        cur.execute("drop role " + username)
        con.commit()


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

    # Can do an assert_raises when we're on 3.8 or above
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


def testApplicatioName(db_kwargs):
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
        match="The parameter application_name can't be of type <class 'int'>.",
    ):
        connect(**db_kwargs)


def test_application_name_bytearray(db_kwargs):
    db_kwargs["application_name"] = bytearray(b"Philby")
    with connect(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256


def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(ProgrammingError, match="3D000"):
        connect(**db_kwargs)


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
