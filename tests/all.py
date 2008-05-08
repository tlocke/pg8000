import unittest

from connection import Tests as ConnectionTests
from query import Tests as QueryTests
from paramstyle import Tests as ParamStyleTests
from dbapi import Tests as DbapiTests
from typeconversion import Tests as TypeTests

if __name__ == "__main__":
    unittest.main()

