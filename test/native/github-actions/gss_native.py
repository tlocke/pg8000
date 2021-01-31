import pg8000.dbapi

import pytest


def test_gss(db_kwargs):
    """ Called by GitHub Actions with auth method gss.
    """

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
            pg8000.dbapi.InterfaceError,
            match="Authentication method 7 not supported by pg8000."):
        pg8000.dbapi.connect(**db_kwargs)
