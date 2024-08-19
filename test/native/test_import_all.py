from pg8000.native import *  # noqa: F403


def test_import_all():
    type(Connection)  # noqa: F405
