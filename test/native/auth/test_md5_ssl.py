import ssl

from pg8000.native import Connection


def test_ssl(db_kwargs):
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    db_kwargs["ssl_context"] = context

    with Connection(**db_kwargs):
        pass
