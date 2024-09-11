from test.utils import parse_server_version


def test_parse_server_version():
    assert parse_server_version("17rc1") == 17
