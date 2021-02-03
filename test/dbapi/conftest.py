import sys

import pg8000.dbapi

import pytest


@pytest.fixture
def con(request, db_kwargs):
    conn = pg8000.dbapi.connect(**db_kwargs)

    def fin():
        conn.rollback()
        try:
            conn.close()
        except pg8000.dbapi.InterfaceError:
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


@pytest.fixture
def is_java():
    return 'java' in sys.platform.lower()
