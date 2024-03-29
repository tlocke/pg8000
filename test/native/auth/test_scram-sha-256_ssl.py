from ssl import CERT_NONE, SSLSocket, create_default_context

import pytest

from pg8000.native import Connection, DatabaseError

# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database pg8000_scram_sha_256

DB = "pg8000_scram_sha_256"


@pytest.fixture
def setup(con):
    try:
        con.run(f"CREATE DATABASE {DB}")
    except DatabaseError:
        pass


def test_scram_sha_256_plus(setup, db_kwargs):
    db_kwargs["database"] = DB

    with Connection(**db_kwargs) as con:
        assert isinstance(con._usock, SSLSocket)


def test_scram_sha_256_plus_ssl_True(setup, db_kwargs):
    db_kwargs["ssl_context"] = True
    db_kwargs["database"] = DB

    with Connection(**db_kwargs) as con:
        assert isinstance(con._usock, SSLSocket)


def test_scram_sha_256_plus_ssl_custom(setup, db_kwargs):
    context = create_default_context()
    context.check_hostname = False
    context.verify_mode = CERT_NONE

    db_kwargs["ssl_context"] = context
    db_kwargs["database"] = DB

    with Connection(**db_kwargs) as con:
        assert isinstance(con._usock, SSLSocket)


def test_scram_sha_256_plus_ssl_False(setup, db_kwargs):
    db_kwargs["ssl_context"] = False
    db_kwargs["database"] = DB

    with Connection(**db_kwargs) as con:
        assert not isinstance(con._usock, SSLSocket)
