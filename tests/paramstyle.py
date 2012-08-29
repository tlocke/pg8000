import unittest
import pg8000

# Tests of the convert_paramstyle function.
class Tests(unittest.TestCase):
    def testQmark(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                "qmark",
                "SELECT ?, ?, \"field_?\" FROM t WHERE a='say ''what?''' "
                    "AND b=? AND c=E'?\\'test\\'?'", (1, 2, 3))
        self.assertEquals(
            new_query,
            "SELECT $1, $2, \"field_?\" FROM t WHERE a='say ''what?''' "
                "AND b=$3 AND c=E'?\\'test\\'?'"
        )
        self.assertEquals(
            new_args,
            (1, 2, 3)
        )

    def testQmark2(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                    "qmark",
                    "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'",
                    (1, 2, 3))
        self.assertEquals(
            new_query,
            "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'"
        )
        self.assertEquals(
            new_args,
            (1, 2, 3)
        )


    def testNumeric(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                    "numeric",
                    "SELECT :2, :1, * FROM t WHERE a=:3", (1, 2, 3))
        self.assertEquals(
                new_query,
                "SELECT $2, $1, * FROM t WHERE a=$3"
        )
        self.assertEquals(
                new_args,
                (1, 2, 3)
        )

    def testNamed(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                    "named",
                    "SELECT :f_2, :f1 FROM t WHERE a=:f_2",
                    {"f_2": 1, "f1": 2})
        self.assertEquals(
            new_query,
            "SELECT $1, $2 FROM t WHERE a=$1"
        )
        self.assertEquals(
            new_args,
            (1, 2)
        )

    def testFormat(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                    "format",
                    "SELECT %s, %s, \"f1_%%\", E'txt_%%' FROM t "
                        "WHERE a=%s AND b='75%%'", (1, 2, 3))
        self.assertEquals(
            new_query,
            "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$3 AND b='75%'"
        )
        self.assertEquals(
            new_args,
            (1, 2, 3)
        )

    def testPyformat(self):
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                    "pyformat",
                    "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' "
                        "FROM t WHERE a=%(f2)s AND b='75%%'",
                    {"f2": 1, "f1": 2, "f3": 3})
        self.assertEquals(
                new_query,
                "SELECT $1, $2, \"f1_%\", E'txt_%' "
                    "FROM t WHERE a=$1 AND b='75%'"
        )
        self.assertEquals(
            new_args,
            (1, 2)
        )

        # pyformat should support %s and an array, too:
        new_query, new_args = pg8000.DBAPI.convert_paramstyle(
                        "pyformat",
                        "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
                            "FROM t WHERE a=%s AND b='75%%'", (1, 2, 3))
        self.assertEquals(
            new_query,
            "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$3 AND b='75%'"
        )
        self.assertEquals(
            new_args,
            (1, 2, 3)
        )

    def testParamIndexErrorQmark(self):
        self.assertRaises(
            pg8000.errors.QueryParameterIndexError,
            pg8000.DBAPI.convert_paramstyle,
            "qmark", "SELECT ?, ?, ?", (1, 2)
        )

    def testParamIndexErrorFormat(self):
        self.assertRaises(
            pg8000.errors.QueryParameterIndexError,
            pg8000.DBAPI.convert_paramstyle,
            "format", "SELECT %s, %s, %s", (1, 2)
        )

    def testParamParseErrorNumeric(self):
        self.assertRaises(
            pg8000.errors.QueryParameterParseError,
            pg8000.DBAPI.convert_paramstyle,
            "numeric", "SELECT :1, :X, :3", (1, 2, 3)
        )

    def testParamParseErrorNamed(self):
        self.assertRaises(
            pg8000.errors.QueryParameterParseError,
            pg8000.DBAPI.convert_paramstyle,
            "named", "SELECT :name1, :*, :name3",
            {"name1": 1, "*": 2, "name3": 3}
        )

if __name__ == "__main__":
    unittest.main()

