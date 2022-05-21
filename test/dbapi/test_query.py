from datetime import datetime as Datetime, timezone as Timezone

import pytest

import pg8000.dbapi
from pg8000.converters import INET_ARRAY, INTEGER


# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.


@pytest.fixture
def db_table(request, con):
    con.paramstyle = "format"
    cursor = con.cursor()
    cursor.execute(
        "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
        "f2 bigint not null, f3 varchar(50) null) "
    )

    def fin():
        try:
            cursor = con.cursor()
            cursor.execute("drop table t1")
        except pg8000.dbapi.DatabaseError:
            pass

    request.addfinalizer(fin)
    return con


def test_database_error(cursor):
    with pytest.raises(pg8000.dbapi.DatabaseError):
        cursor.execute("INSERT INTO t99 VALUES (1, 2, 3)")


def test_parallel_queries(db_table):
    cursor = db_table.cursor()
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))
    c1 = db_table.cursor()
    c2 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1")
    for row in c1.fetchall():
        f1, f2, f3 = row
        c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
        for row in c2.fetchall():
            f1, f2, f3 = row


def test_parallel_open_portals(con):
    c1 = con.cursor()
    c2 = con.cursor()
    c1count, c2count = 0, 0
    q = "select * from generate_series(1, %s)"
    params = (100,)
    c1.execute(q, params)
    c2.execute(q, params)
    for c2row in c2.fetchall():
        c2count += 1
    for c1row in c1.fetchall():
        c1count += 1

    assert c1count == c2count


# Run a query on a table, alter the structure of the table, then run the
# original query again.


def test_alter(db_table):
    cursor = db_table.cursor()
    cursor.execute("select * from t1")
    cursor.execute("alter table t1 drop column f3")
    cursor.execute("select * from t1")


# Run a query on a table, drop then re-create the table, then run the
# original query again.


def test_create(db_table):
    cursor = db_table.cursor()
    cursor.execute("select * from t1")
    cursor.execute("drop table t1")
    cursor.execute("create temporary table t1 (f1 int primary key)")
    cursor.execute("select * from t1")


def test_insert_returning(db_table):
    cursor = db_table.cursor()
    cursor.execute("CREATE TEMPORARY TABLE t2 (id serial, data text)")

    # Test INSERT ... RETURNING with one row...
    cursor.execute("INSERT INTO t2 (data) VALUES (%s) RETURNING id", ("test1",))
    row_id = cursor.fetchone()[0]
    cursor.execute("SELECT data FROM t2 WHERE id = %s", (row_id,))
    assert "test1" == cursor.fetchone()[0]

    assert cursor.rowcount == 1

    # Test with multiple rows...
    cursor.execute(
        "INSERT INTO t2 (data) VALUES (%s), (%s), (%s) " "RETURNING id",
        ("test2", "test3", "test4"),
    )
    assert cursor.rowcount == 3
    ids = tuple([x[0] for x in cursor.fetchall()])
    assert len(ids) == 3


def test_row_count(db_table):
    cursor = db_table.cursor()
    expected_count = 57
    cursor.executemany(
        "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
        tuple((i, i, None) for i in range(expected_count)),
    )

    # Check rowcount after executemany
    assert expected_count == cursor.rowcount

    cursor.execute("SELECT * FROM t1")

    # Check row_count without doing any reading first...
    assert expected_count == cursor.rowcount

    # Check rowcount after reading some rows, make sure it still
    # works...
    for i in range(expected_count // 2):
        cursor.fetchone()
    assert expected_count == cursor.rowcount

    cursor = db_table.cursor()
    # Restart the cursor, read a few rows, and then check rowcount
    # again...
    cursor.execute("SELECT * FROM t1")
    for i in range(expected_count // 3):
        cursor.fetchone()
    assert expected_count == cursor.rowcount

    # Should be -1 for a command with no results
    cursor.execute("DROP TABLE t1")
    assert -1 == cursor.rowcount


def test_row_count_update(db_table):
    cursor = db_table.cursor()
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))
    cursor.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
    assert cursor.rowcount == 2


def test_int_oid(cursor):
    # https://bugs.launchpad.net/pg8000/+bug/230796
    cursor.execute("SELECT typname FROM pg_type WHERE oid = %s", (100,))


def test_unicode_query(cursor):
    cursor.execute(
        "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
        "(\u0438\u043c\u044f VARCHAR(50), "
        "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))"
    )


def test_executemany(db_table):
    cursor = db_table.cursor()
    cursor.executemany(
        "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
        ((1, 1, "Avast ye!"), (2, 1, None)),
    )

    cursor.executemany(
        "select CAST(%s AS TIMESTAMP)",
        ((Datetime(2014, 5, 7, tzinfo=Timezone.utc),), (Datetime(2014, 5, 7),)),
    )


def test_executemany_setinputsizes(cursor):
    """Make sure that setinputsizes works for all the parameter sets"""

    cursor.execute(
        "CREATE TEMPORARY TABLE t1 (f1 int primary key, f2 inet[] not null) "
    )

    cursor.setinputsizes(INTEGER, INET_ARRAY)
    cursor.executemany(
        "INSERT INTO t1 (f1, f2) VALUES (%s, %s)", ((1, ["1.1.1.1"]), (2, ["0.0.0.0"]))
    )


def test_executemany_no_param_sets(cursor):
    cursor.executemany("INSERT INTO t1 (f1, f2) VALUES (%s, %s)", [])
    assert cursor.rowcount == -1


# Check that autocommit stays off
# We keep track of whether we're in a transaction or not by using the
# READY_FOR_QUERY message.
def test_transactions(db_table):
    cursor = db_table.cursor()
    cursor.execute("commit")
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, "Zombie"))
    cursor.execute("rollback")
    cursor.execute("select * from t1")

    assert cursor.rowcount == 0


def test_in(cursor):
    cursor.execute("SELECT typname FROM pg_type WHERE oid = any(%s)", ([16, 23],))
    ret = cursor.fetchall()
    assert ret[0][0] == "bool"


def test_no_previous_tpc(con):
    con.tpc_begin("Stacey")
    cursor = con.cursor()
    cursor.execute("SELECT * FROM pg_type")
    con.tpc_commit()


# Check that tpc_recover() doesn't start a transaction
def test_tpc_recover(con):
    con.tpc_recover()
    cursor = con.cursor()
    con.autocommit = True

    # If tpc_recover() has started a transaction, this will fail
    cursor.execute("VACUUM")


def test_tpc_prepare(con):
    xid = "Stacey"
    con.tpc_begin(xid)
    con.tpc_prepare()
    con.tpc_rollback(xid)


def test_empty_query(cursor):
    """No exception thrown"""
    cursor.execute("")


# rolling back when not in a transaction doesn't generate a warning
def test_rollback_no_transaction(con):
    # Remove any existing notices
    con.notices.clear()

    # First, verify that a raw rollback does produce a notice
    con.execute_unnamed("rollback")

    assert 1 == len(con.notices)

    # 25P01 is the code for no_active_sql_tronsaction. It has
    # a message and severity name, but those might be
    # localized/depend on the server version.
    assert con.notices.pop().get(b"C") == b"25P01"

    # Now going through the rollback method doesn't produce
    # any notices because it knows we're not in a transaction.
    con.rollback()

    assert 0 == len(con.notices)


def test_setinputsizes(con):
    cursor = con.cursor()
    cursor.setinputsizes(20)
    cursor.execute("select %s", (None,))
    retval = cursor.fetchall()
    assert retval[0][0] is None


def test_unexecuted_cursor_rowcount(con):
    cursor = con.cursor()
    assert cursor.rowcount == -1


def test_unexecuted_cursor_description(con):
    cursor = con.cursor()
    assert cursor.description is None


def test_callproc(pg_version, cursor):
    if pg_version > 10:
        cursor.execute(
            """
CREATE PROCEDURE echo(INOUT val text)
  LANGUAGE plpgsql AS
$proc$
BEGIN
END
$proc$;
"""
        )

        cursor.callproc("echo", ["hello"])
        assert cursor.fetchall() == (["hello"],)


def test_null_result(db_table):
    cur = db_table.cursor()
    cur.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, "a"))
    with pytest.raises(pg8000.dbapi.ProgrammingError):
        cur.fetchall()


def test_not_parsed_if_no_params(mocker, cursor):
    mock_convert_paramstyle = mocker.patch("pg8000.dbapi.convert_paramstyle")
    cursor.execute("ROLLBACK")
    mock_convert_paramstyle.assert_not_called()
