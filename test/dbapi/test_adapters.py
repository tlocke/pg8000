from pg8000 import converters


def test_null_out():
    assert converters.null_out(None) is None
