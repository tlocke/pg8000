import datetime
import os
import time

import pytest

from pg8000.dbapi import (
    BINARY,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)


@pytest.fixture
def has_tzset():
    # Neither Windows nor Jython 2.5.3 have a time.tzset() so skip
    if hasattr(time, "tzset"):
        os.environ["TZ"] = "UTC"
        time.tzset()
        return True
    return False


# DBAPI compatible interface tests
@pytest.fixture
def db_table(con, has_tzset):
    c = con.cursor()
    c.execute(
        "CREATE TEMPORARY TABLE t1 "
        "(f1 int primary key, f2 int not null, f3 varchar(50) null) "
        "ON COMMIT DROP"
    )
    c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
    c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
    c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
    c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
    c.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))
    return con


def test_parallel_queries(db_table):
    c1 = db_table.cursor()
    c2 = db_table.cursor()

    c1.execute("SELECT f1, f2, f3 FROM t1")
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row
        c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
        while 1:
            row = c2.fetchone()
            if row is None:
                break
            f1, f2, f3 = row


def test_qmark(mocker, db_table):
    mocker.patch("pg8000.dbapi.paramstyle", "qmark")
    c1 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (3,))
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row


def test_numeric(mocker, db_table):
    mocker.patch("pg8000.dbapi.paramstyle", "numeric")
    c1 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row


def test_named(mocker, db_table):
    mocker.patch("pg8000.dbapi.paramstyle", "named")
    c1 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row


def test_format(mocker, db_table):
    mocker.patch("pg8000.dbapi.paramstyle", "format")
    c1 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row


def test_pyformat(mocker, db_table):
    mocker.patch("pg8000.dbapi.paramstyle", "pyformat")
    c1 = db_table.cursor()
    c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
    while 1:
        row = c1.fetchone()
        if row is None:
            break
        f1, f2, f3 = row


def test_arraysize(db_table):
    c1 = db_table.cursor()
    c1.arraysize = 3
    c1.execute("SELECT * FROM t1")
    retval = c1.fetchmany()
    assert len(retval) == c1.arraysize


def test_date():
    val = Date(2001, 2, 3)
    assert val == datetime.date(2001, 2, 3)


def test_time():
    val = Time(4, 5, 6)
    assert val == datetime.time(4, 5, 6)


def test_timestamp():
    val = Timestamp(2001, 2, 3, 4, 5, 6)
    assert val == datetime.datetime(2001, 2, 3, 4, 5, 6)


def test_date_from_ticks(has_tzset):
    if has_tzset:
        val = DateFromTicks(1173804319)
        assert val == datetime.date(2007, 3, 13)


def testTimeFromTicks(has_tzset):
    if has_tzset:
        val = TimeFromTicks(1173804319)
        assert val == datetime.time(16, 45, 19)


def test_timestamp_from_ticks(has_tzset):
    if has_tzset:
        val = TimestampFromTicks(1173804319)
        assert val == datetime.datetime(2007, 3, 13, 16, 45, 19)


def test_binary():
    v = Binary(b"\x00\x01\x02\x03\x02\x01\x00")
    assert v == b"\x00\x01\x02\x03\x02\x01\x00"
    assert isinstance(v, BINARY)


def test_row_count(db_table):
    c1 = db_table.cursor()
    c1.execute("SELECT * FROM t1")

    assert 5 == c1.rowcount

    c1.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
    assert 2 == c1.rowcount

    c1.execute("DELETE FROM t1")
    assert 5 == c1.rowcount


def test_fetch_many(db_table):
    cursor = db_table.cursor()
    cursor.arraysize = 2
    cursor.execute("SELECT * FROM t1")
    assert 2 == len(cursor.fetchmany())
    assert 2 == len(cursor.fetchmany())
    assert 1 == len(cursor.fetchmany())
    assert 0 == len(cursor.fetchmany())


def test_iterator(db_table):
    cursor = db_table.cursor()
    cursor.execute("SELECT * FROM t1 ORDER BY f1")
    f1 = 0
    for row in cursor.fetchall():
        next_f1 = row[0]
        assert next_f1 > f1
        f1 = next_f1


# Vacuum can't be run inside a transaction, so we need to turn
# autocommit on.
def test_vacuum(con):
    con.autocommit = True
    cursor = con.cursor()
    cursor.execute("vacuum")


def test_prepared_statement(con):
    cursor = con.cursor()
    cursor.execute("PREPARE gen_series AS SELECT generate_series(1, 10);")
    cursor.execute("EXECUTE gen_series")


def test_cursor_type(cursor):
    assert str(type(cursor)) == "<class 'pg8000.dbapi.Cursor'>"
