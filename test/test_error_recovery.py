import datetime
import warnings

import pg8000

import pytest


class PG8000TestException(Exception):
    pass


def raise_exception(val):
    raise PG8000TestException("oh noes!")


def test_py_value_fail(con, mocker):
    # Ensure that if types.py_value throws an exception, the original
    # exception is raised (PG8000TestException), and the connection is
    # still usable after the error.
    mocker.patch.object(con, 'py_types')
    con.py_types = {
        datetime.time: (1083, raise_exception)
    }

    with con.cursor() as c, pytest.raises(PG8000TestException):
        c.execute("SELECT %s as f1", (datetime.time(10, 30),))
        c.fetchall()

        # ensure that the connection is still usable for a new query
        c.execute("VALUES ('hw3'::text)")
        assert c.fetchone()[0] == "hw3"


def test_no_data_error_recovery(con):
    for i in range(1, 4):
        with con.cursor() as c, pytest.raises(pg8000.DatabaseError) as e:
            c.execute("DROP TABLE t1")
        assert e.value.args[0]['C'] == '42P01'
        con.rollback()


def testClosedConnection(db_kwargs):
    warnings.simplefilter("ignore")
    my_db = pg8000.connect(**db_kwargs)
    cursor = my_db.cursor()
    my_db.close()
    with pytest.raises(my_db.InterfaceError, match="connection is closed"):
        cursor.execute("VALUES ('hw1'::text)")

    warnings.resetwarnings()
