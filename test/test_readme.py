import doctest

from pathlib import Path


def test_readme():
    doctest.testfile(
        str(Path("..") / "README.adoc"), verbose=False, optionflags=doctest.ELLIPSIS
    )
