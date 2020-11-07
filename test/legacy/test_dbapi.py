import datetime
import os
import time

import pg8000

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
    with con.cursor() as c:
        c.execute(
            "CREATE TEMPORARY TABLE t1 "
            "(f1 int primary key, f2 int not null, f3 varchar(50) null) "
            "ON COMMIT DROP")
        c.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        c.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
        c.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
        c.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
        c.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
            (5, 10000, None))
    return con


def test_parallel_queries(db_table):
    with db_table.cursor() as c1, db_table.cursor() as c2:

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


def test_qmark(db_table):
    orig_paramstyle = pg8000.paramstyle
    try:
        pg8000.paramstyle = "qmark"
        with db_table.cursor() as c1:
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
    finally:
        pg8000.paramstyle = orig_paramstyle


def test_numeric(db_table):
    orig_paramstyle = pg8000.paramstyle
    try:
        pg8000.paramstyle = "numeric"
        with db_table.cursor() as c1:
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
    finally:
        pg8000.paramstyle = orig_paramstyle


def test_named(db_table):
    orig_paramstyle = pg8000.paramstyle
    try:
        pg8000.paramstyle = "named"
        with db_table.cursor() as c1:
            c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
    finally:
        pg8000.paramstyle = orig_paramstyle


def test_format(db_table):
    orig_paramstyle = pg8000.paramstyle
    try:
        pg8000.paramstyle = "format"
        with db_table.cursor() as c1:
            c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
    finally:
        pg8000.paramstyle = orig_paramstyle


def test_pyformat(db_table):
    orig_paramstyle = pg8000.paramstyle
    try:
        pg8000.paramstyle = "pyformat"
        with db_table.cursor() as c1:
            c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
            while 1:
                row = c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
    finally:
        pg8000.paramstyle = orig_paramstyle


def test_arraysize(db_table):
    with db_table.cursor() as c1:
        c1.arraysize = 3
        c1.execute("SELECT * FROM t1")
        retval = c1.fetchmany()
        assert len(retval) == c1.arraysize


def test_date():
    val = pg8000.Date(2001, 2, 3)
    assert val == datetime.date(2001, 2, 3)


def test_time():
    val = pg8000.Time(4, 5, 6)
    assert val == datetime.time(4, 5, 6)


def test_timestamp():
    val = pg8000.Timestamp(2001, 2, 3, 4, 5, 6)
    assert val == datetime.datetime(2001, 2, 3, 4, 5, 6)


def test_date_from_ticks(has_tzset):
    if has_tzset:
        val = pg8000.DateFromTicks(1173804319)
        assert val == datetime.date(2007, 3, 13)


def testTimeFromTicks(has_tzset):
    if has_tzset:
        val = pg8000.TimeFromTicks(1173804319)
        assert val == datetime.time(16, 45, 19)


def test_timestamp_from_ticks(has_tzset):
    if has_tzset:
        val = pg8000.TimestampFromTicks(1173804319)
        assert val == datetime.datetime(2007, 3, 13, 16, 45, 19)


def test_binary():
    v = pg8000.Binary(b"\x00\x01\x02\x03\x02\x01\x00")
    assert v == b"\x00\x01\x02\x03\x02\x01\x00"
    assert isinstance(v, pg8000.BINARY)


def test_row_count(db_table):
    with db_table.cursor() as c1:
        c1.execute("SELECT * FROM t1")

        assert 5 == c1.rowcount

        c1.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
        assert 2 == c1.rowcount

        c1.execute("DELETE FROM t1")
        assert 5 == c1.rowcount


def test_fetch_many(db_table):
    with db_table.cursor() as cursor:
        cursor.arraysize = 2
        cursor.execute("SELECT * FROM t1")
        assert 2 == len(cursor.fetchmany())
        assert 2 == len(cursor.fetchmany())
        assert 1 == len(cursor.fetchmany())
        assert 0 == len(cursor.fetchmany())


def test_iterator(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("SELECT * FROM t1 ORDER BY f1")
        f1 = 0
        for row in cursor:
            next_f1 = row[0]
            assert next_f1 > f1
            f1 = next_f1


# Vacuum can't be run inside a transaction, so we need to turn
# autocommit on.
def test_vacuum(con):
    con.autocommit = True
    with con.cursor() as cursor:
        cursor.execute("vacuum")


def test_prepared_statement(con):
    with con.cursor() as cursor:
        cursor.execute('PREPARE gen_series AS SELECT generate_series(1, 10);')
        cursor.execute('EXECUTE gen_series')


def test_cursor_type(cursor):
    assert str(type(cursor)) == "<class 'pg8000.legacy.Cursor'>"
