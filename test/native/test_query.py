from warnings import filterwarnings

import pg8000.native

import pytest


# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.

@pytest.fixture
def db_table(request, con):
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = 'format'
    con.run(
        "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
        "f2 bigint not null, f3 varchar(50) null) ")

    def fin():
        try:
            con.run("drop table t1")
        except pg8000.native.DatabaseError:
            pass

    request.addfinalizer(fin)
    return con


def test_database_error(con):
    with pytest.raises(pg8000.native.DatabaseError):
        con.run("INSERT INTO t99 VALUES (1, 2, 3)")


# Run a query on a table, alter the structure of the table, then run the
# original query again.


def test_alter(db_table):
    db_table.run("select * from t1")
    db_table.run("alter table t1 drop column f3")
    db_table.run("select * from t1")


# Run a query on a table, drop then re-create the table, then run the
# original query again.

def test_create(db_table):
    db_table.run("select * from t1")
    db_table.run("drop table t1")
    db_table.run("create temporary table t1 (f1 int primary key)")
    db_table.run("select * from t1")


def test_insert_returning(db_table):
    db_table.run("CREATE TEMPORARY TABLE t2 (id serial, data text)")

    # Test INSERT ... RETURNING with one row...
    res = db_table.run(
        "INSERT INTO t2 (data) VALUES (:v) RETURNING id", v="test1")
    row_id = res[0][0]
    res = db_table.run("SELECT data FROM t2 WHERE id = :v", v=row_id)
    assert "test1" == res[0][0]

    assert db_table.row_count == 1

    # Test with multiple rows...
    res = db_table.run(
        "INSERT INTO t2 (data) VALUES (:v1), (:v2), (:v3) "
        "RETURNING id", v1="test2", v2="test3", v3="test4")
    assert db_table.row_count == 3
    ids = [x[0] for x in res]
    assert len(ids) == 3


def test_row_count(db_table):
    expected_count = 57
    for i in range(expected_count):
        db_table.run(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
            v1=i, v2=i, v3=None)

    db_table.run("SELECT * FROM t1")

    # Check row_count
    assert expected_count == db_table.row_count

    # Should be -1 for a command with no results
    db_table.run("DROP TABLE t1")
    assert -1 == db_table.row_count


def test_row_count_update(db_table):
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=1, v2=1,
        v3=None)
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=2, v2=10,
        v3=None)
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=3, v2=100,
        v3=None)
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=4, v2=1000,
        v3=None)
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=5, v2=10000,
        v3=None)
    db_table.run("UPDATE t1 SET f3 = :v1 WHERE f2 > 101", v1="Hello!")
    assert db_table.row_count == 2


def test_int_oid(con):
    # https://bugs.launchpad.net/pg8000/+bug/230796
    con.run("SELECT typname FROM pg_type WHERE oid = :v", v=100)


def test_unicode_query(con):
    con.run(
        "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
        "(\u0438\u043c\u044f VARCHAR(50), "
        "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))")


def test_transactions(db_table):
    db_table.run("start transaction")
    db_table.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)", v1=1, v2=1,
        v3="Zombie")
    db_table.run("rollback")
    db_table.run("select * from t1")

    assert db_table.row_count == 0


def test_in(con):
    ret = con.run(
        "SELECT typname FROM pg_type WHERE oid = any(:v)", v=[16, 23])
    assert ret[0][0] == 'bool'


# An empty query should raise a ProgrammingError
def test_empty_query(con):
    with pytest.raises(pg8000.native.DatabaseError):
        con.run("")


def test_rollback_no_transaction(con):
    # Remove any existing notices
    con.notices.clear()

    # First, verify that a raw rollback does produce a notice
    con.run("rollback")

    assert 1 == len(con.notices)

    # 25P01 is the code for no_active_sql_tronsaction. It has
    # a message and severity name, but those might be
    # localized/depend on the server version.
    assert con.notices.pop().get(b'C') == b'25P01'


def test_close_prepared_statement(con):
    ps = con.prepare("select 1")
    ps.run()
    res = con.run("select count(*) from pg_prepared_statements")
    assert res[0][0] == 1  # Should have one prepared statement

    ps.close()

    res = con.run("select count(*) from pg_prepared_statements")
    assert res[0][0] == 0  # Should have no prepared statements


def test_unexecuted_connection_row_count(con):
    assert con.row_count is None


def test_unexecuted_connection_columns(con):
    assert con.columns is None
