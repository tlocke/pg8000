from io import BytesIO

from pg8000.core import CoreConnection


def test_handle_AUTHENTICATION_3(mocker):
    """Shouldn't send a FLUSH message, as FLUSH only used in extended-query"""

    mocker.patch.object(CoreConnection, "__init__", lambda x: None)
    con = CoreConnection()
    con.password = "barbour".encode("utf8")
    con._flush = mocker.Mock()
    buf = BytesIO()
    con._write = buf.write
    CoreConnection.handle_AUTHENTICATION_REQUEST(con, b"\x00\x00\x00\x03", None)
    assert buf.getvalue() == b"p\x00\x00\x00\x0cbarbour\x00"
