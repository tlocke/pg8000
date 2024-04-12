import pytest

from pg8000 import DatabaseError, connect

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

    with connect(**db_kwargs):
        pass
