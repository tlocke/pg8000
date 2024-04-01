import pytest

from pg8000.native import Connection, DatabaseError, InterfaceError

# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database pg8000_scram_sha_256

DB = "pg8000_scram_sha_256"


@pytest.fixture
def setup(con):
    try:
        con.run(f"CREATE DATABASE {DB}")
    except DatabaseError:
        pass
    con.run("ALTER SYSTEM SET ssl = off")
    con.run("SELECT pg_reload_conf()")
    yield
    con.run("ALTER SYSTEM SET ssl = on")
    con.run("SELECT pg_reload_conf()")


def test_scram_sha_256(setup, db_kwargs):
    db_kwargs["database"] = DB

    with Connection(**db_kwargs):
        pass


def test_scram_sha_256_ssl_False(setup, db_kwargs):
    db_kwargs["database"] = DB
    db_kwargs["ssl_context"] = False

    with Connection(**db_kwargs):
        pass


def test_scram_sha_256_ssl_True(setup, db_kwargs):
    db_kwargs["database"] = DB
    db_kwargs["ssl_context"] = True

    with pytest.raises(InterfaceError, match="Server refuses SSL"):
        with Connection(**db_kwargs):
            pass
