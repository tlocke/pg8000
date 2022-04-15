import ssl

import pytest

import pg8000.legacy


def testGss(db_kwargs):
    """This requires a line in pg_hba.conf that requires gss for the database
    pg8000_gss
    """
    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
        pg8000.legacy.InterfaceError,
        match="Authentication method 7 not supported by pg8000.",
    ):
        pg8000.legacy.connect(**db_kwargs)


def test_ssl(db_kwargs):
    context = ssl.create_default_context()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    with pg8000.connect(**db_kwargs):
        pass
