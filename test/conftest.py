import sys
from os import environ

import pg8000

import pytest


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        'user': 'postgres',
        'password': 'pw'
    }

    try:
        db_connect['port'] = int(environ['PGPORT'])
    except KeyError:
        pass
    return db_connect


@pytest.fixture
def con(request, db_kwargs):
    conn = pg8000.connect(**db_kwargs)

    def fin():
        conn.rollback()
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


@pytest.fixture
def is_java():
    return 'java' in sys.platform.lower()
