import pytest

from pg8000.dbapi import DatabaseError, connect

# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256


def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        with connect(**db_kwargs) as con:
            con.close()
