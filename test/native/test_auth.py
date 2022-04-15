import ssl

import pytest

import pg8000.native


def test_gss(db_kwargs):
    """This requires a line in pg_hba.conf that requires gss for the database
    pg8000_gss
    """

    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
        pg8000.native.InterfaceError,
        match="Authentication method 7 not supported by pg8000.",
    ):
        pg8000.native.Connection(**db_kwargs)


def test_ssl(db_kwargs):
    context = ssl.create_default_context()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    with pg8000.native.Connection(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram_sha_256


def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.native.DatabaseError, match="3D000"):
        pg8000.native.Connection(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram_sha_256


def test_scram_sha_256_plus(db_kwargs):
    context = ssl.SSLContext()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.native.DatabaseError, match="3D000"):
        pg8000.native.Connection(**db_kwargs)
