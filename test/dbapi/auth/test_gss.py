import pytest

from pg8000.dbapi import InterfaceError, connect


# This requires a line in pg_hba.conf that requires gss for the database
# pg8000_gss


def test_gss(db_kwargs):
    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
        InterfaceError,
        match="Authentication method 7 not supported by pg8000.",
    ):
        with connect(**db_kwargs):
            pass
