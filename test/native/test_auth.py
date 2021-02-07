import ssl
import sys

import pg8000.native

import pytest


def test_gss(db_kwargs):
    """ This requires a line in pg_hba.conf that requires gss for the database
    pg8000_gss
    """

    db_kwargs["database"] = "pg8000_gss"

    # Should raise an exception saying gss isn't supported
    with pytest.raises(
            pg8000.native.InterfaceError,
            match="Authentication method 7 not supported by pg8000."):
        pg8000.native.Connection(**db_kwargs)


# Check if running in Jython
if 'java' in sys.platform:
    from javax.net.ssl import TrustManager, X509TrustManager
    from jarray import array
    from javax.net.ssl import SSLContext

    class TrustAllX509TrustManager(X509TrustManager):
        '''Define a custom TrustManager which will blindly accept all
        certificates'''

        def checkClientTrusted(self, chain, auth):
            pass

        def checkServerTrusted(self, chain, auth):
            pass

        def getAcceptedIssuers(self):
            return None
    # Create a static reference to an SSLContext which will use
    # our custom TrustManager
    trust_managers = array([TrustAllX509TrustManager()], TrustManager)
    TRUST_ALL_CONTEXT = SSLContext.getInstance("SSL")
    TRUST_ALL_CONTEXT.init(None, trust_managers, None)
    # Keep a static reference to the JVM's default SSLContext for restoring
    # at a later time
    DEFAULT_CONTEXT = SSLContext.getDefault()


@pytest.fixture
def trust_all_certificates(request):
    '''Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates'''
    # Only do this if running under Jython
    is_java = 'java' in sys.platform

    if is_java:
        from javax.net.ssl import SSLContext
        SSLContext.setDefault(TRUST_ALL_CONTEXT)

    def fin():
        if is_java:
            SSLContext.setDefault(DEFAULT_CONTEXT)

    request.addfinalizer(fin)


@pytest.mark.usefixtures("trust_all_certificates")
def test_ssl(db_kwargs):
    context = ssl.SSLContext()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    with pg8000.native.Connection(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram_sha_256

def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "pg8000_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.native.DatabaseError, match='3D000'):
        pg8000.native.Connection(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram_sha_256

def test_scram_sha_256_plus(db_kwargs):
    context = ssl.SSLContext()
    context.check_hostname = False
    db_kwargs["ssl_context"] = context
    db_kwargs["database"] = "pg8000_scram_sha_256"

    print(db_kwargs)
    # Should only raise an exception saying db doesn't exist
    with pytest.raises(pg8000.native.DatabaseError, match='3D000'):
        pg8000.native.Connection(**db_kwargs)
