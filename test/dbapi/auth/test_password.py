import pytest

from pg8000.dbapi import DatabaseError, connect

# This requires a line in pg_hba.conf that requires 'password' for the
# database pg8000_password


def test_password(db_kwargs):
    db_kwargs["database"] = "pg8000_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        with connect(**db_kwargs):
            pass
