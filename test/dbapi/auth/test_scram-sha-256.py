import pytest

from pg8000.dbapi import DatabaseError, connect

# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database pg8000_scram_sha_256

DB = "pg8000_scram_sha_256"


@pytest.fixture
def setup(con, cursor):
    con.autocommit = True
    try:
        cursor.execute(f"CREATE DATABASE {DB}")
    except DatabaseError:
        con.rollback()

    cursor.execute("ALTER SYSTEM SET ssl = off")
    cursor.execute("SELECT pg_reload_conf()")
    yield
    cursor.execute("ALTER SYSTEM SET ssl = on")
    cursor.execute("SELECT pg_reload_conf()")


def test_scram_sha_256(setup, db_kwargs):
    db_kwargs["database"] = DB

    with connect(**db_kwargs):
        pass
