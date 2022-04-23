import pytest

from pg8000.dbapi import DatabaseError, connect


# This requires a line in pg_hba.conf that requires md5 for the database
# pg8000_md5


def test_md5(db_kwargs):
    db_kwargs["database"] = "pg8000_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(DatabaseError, match="3D000"):
        with connect(**db_kwargs):
            pass
