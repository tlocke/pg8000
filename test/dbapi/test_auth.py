import pg8000.dbapi

import pytest


# This requires a line in pg_hba.conf that requires md5 for the database
# pg8000_md5

def testMd5(db_kwargs):
    db_kwargs["database"] = "pg8000_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.dbapi.DatabaseError, match='3D000'):
        pg8000.dbapi.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires gss for the database
# pg8000_gss

def testGss(db_kwargs):
    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
            pg8000.dbapi.InterfaceError,
            match="Authentication method 7 not supported by pg8000."):
        pg8000.dbapi.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires 'password' for the
# database pg8000_password

def testPassword(db_kwargs):
    db_kwargs["database"] = "pg8000_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.dbapi.DatabaseError, match='3D000'):
        pg8000.dbapi.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256

def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.dbapi.DatabaseError, match='3D000'):
        pg8000.dbapi.connect(**db_kwargs)
