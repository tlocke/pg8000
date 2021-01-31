import ssl
import sys

import pg8000.dbapi

import pytest


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
    with pg8000.dbapi.connect(**db_kwargs):
        pass
