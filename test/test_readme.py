import doctest

from pathlib import Path


def test_readme():
    failure_count, _ = doctest.testfile(
        str(Path("..") / "README.rst"), verbose=False, optionflags=doctest.ELLIPSIS
    )
    assert failure_count == 0
