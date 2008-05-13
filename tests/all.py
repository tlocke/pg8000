import unittest

from connection import Tests as ConnectionTests
from query import Tests as QueryTests
from paramstyle import Tests as ParamStyleTests
from dbapi import Tests as DbapiTests
from typeconversion import Tests as TypeTests
from pg8000_dbapi20 import Tests as Dbapi20Tests
from error_recovery import Tests as ErrorRecoveryTests

if __name__ == "__main__":
    unittest.main()

