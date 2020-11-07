from pg8000 import PGInterval


def test_pginterval_constructor_days():
    i = PGInterval(days=1)
    assert i.months is None
    assert i.days == 1
    assert i.microseconds is None
