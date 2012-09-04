import unittest
from pg8000 import util

# Tests of the convert_paramstyle function.
class ParamFormatTests(unittest.TestCase):

    def test_coerce_positional(self):
        query = "SELECT %s, %s FROM table WHERE foo=%s AND bar=%s"
        params = (1, 2, 3, 4)

        converted_query, param_fn = util.coerce_positional(query, params)
        self.assertEquals(
            converted_query,
            "SELECT $1, $2 FROM table WHERE foo=$3 AND bar=$4"
        )
        self.assertEquals(
            param_fn(params),
            (1, 2, 3, 4)
        )

    def test_coerce_named(self):
        query = "SELECT %(q)s, %(p)s FROM table WHERE foo=%(z)s AND bar=%(x)s"
        params = {"q": 1, "p": 2, "z": 3, "x": 4}

        converted_query, param_fn = util.coerce_named(query, params)
        self.assertEquals(
            converted_query,
            "SELECT $1, $2 FROM table WHERE foo=$3 AND bar=$4"
        )
        self.assertEquals(
            param_fn(params),
            [1, 2, 3, 4]
        )


