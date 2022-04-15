from os import environ

import pytest

import pg8000


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {"user": "postgres", "password": "pw"}

    for kw, var, f in [
        ("host", "PGHOST", str),
        ("password", "PGPASSWORD", str),
        ("port", "PGPORT", int),
    ]:
        try:
            db_connect[kw] = f(environ[var])
        except KeyError:
            pass

    return db_connect


@pytest.fixture
def con(request, db_kwargs):
    conn = pg8000.connect(**db_kwargs)

    def fin():
        try:
            conn.rollback()
        except pg8000.InterfaceError:
            pass

        try:
            conn.close()
        except pg8000.InterfaceError:
            pass

    request.addfinalizer(fin)
    return conn


@pytest.fixture
def cursor(request, con):
    cursor = con.cursor()

    def fin():
        cursor.close()

    request.addfinalizer(fin)
    return cursor
