import ssl

import pg8000.dbapi


def test_ssl(db_kwargs):
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    db_kwargs["ssl_context"] = context
    with pg8000.dbapi.connect(**db_kwargs):
        pass
