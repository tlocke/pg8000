import os
import time

import pytest


@pytest.fixture
def has_tzset():

    # Neither Windows nor Jython 2.5.3 have a time.tzset() so skip
    if hasattr(time, 'tzset'):
        os.environ['TZ'] = "UTC"
        time.tzset()
        return True
    return False


# DBAPI compatible interface tests
@pytest.fixture
def db_table(con, has_tzset):
    con.run("START TRANSACTION")
    con.run(
        "CREATE TEMPORARY TABLE t1 "
        "(f1 int primary key, f2 int not null, f3 varchar(50) null) "
        "ON COMMIT DROP")
    con.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
        v1=1, v2=1, v3=None)
    con.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
        v1=2, v2=10, v3=None)
    con.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
        v1=3, v2=100, v3=None)
    con.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
        v1=4, v2=1000, v3=None)
    con.run(
        "INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v2, :v3)",
        v1=5, v2=10000, v3=None)
    return con


def test_named(db_table):
    res = db_table.run("SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", f1=3)
    for row in res:
        f1, f2, f3 = row


def test_row_count(db_table):
    db_table.run("SELECT * FROM t1")

    assert 5 == db_table.row_count

    db_table.run("UPDATE t1 SET f3 = :v WHERE f2 > 101", v="Hello!")
    assert 2 == db_table.row_count

    db_table.run("DELETE FROM t1")
    assert 5 == db_table.row_count


def test_prepared_statement(con):
    con.run('PREPARE gen_series AS SELECT generate_series(1, 10);')
    con.run('EXECUTE gen_series')
