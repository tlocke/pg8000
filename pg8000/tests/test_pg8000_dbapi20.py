#!/usr/bin/env python
from . import dbapi20
import unittest
import pg8000
from .connection_settings import db_connect


class Tests(dbapi20.DatabaseAPI20Test):
    driver = pg8000
    connect_args = ()
    connect_kw_args = db_connect

    lower_func = 'lower'  # For stored procedure test

    def test_nextset(self):
        pass

    def test_setoutputsize(self):
        pass

if __name__ == '__main__':
    unittest.main()
