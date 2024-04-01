from ssl import CERT_NONE, create_default_context
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


def test_scram_sha_256(setup, db_kwargs):
    db_kwargs["database"] = DB

    con = connect(**db_kwargs)
    con.close()


def test_scram_sha_256_ssl_context(setup, db_kwargs):
    ssl_context = create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = CERT_NONE

    db_kwargs["database"] = DB
    db_kwargs["ssl_context"] = ssl_context

    con = connect(**db_kwargs)
    con.close()
