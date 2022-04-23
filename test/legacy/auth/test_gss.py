import pytest

from pg8000 import InterfaceError, connect


def test_gss(db_kwargs):
    """This requires a line in pg_hba.conf that requires gss for the database
    pg8000_gss
    """
    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
        InterfaceError,
        match="Authentication method 7 not supported by pg8000.",
    ):
        connect(**db_kwargs)
