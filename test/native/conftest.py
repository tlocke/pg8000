import sys

import pg8000.native

import pytest


@pytest.fixture
def con(request, db_kwargs):
    conn = pg8000.native.Connection(**db_kwargs)

    def fin():
        conn.run("rollback")
        try:
            conn.close()
        except pg8000.native.InterfaceError:
            pass

    request.addfinalizer(fin)
    return conn


@pytest.fixture
def is_java():
    return 'java' in sys.platform.lower()
