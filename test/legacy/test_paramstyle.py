from pg8000.legacy import convert_paramstyle as convert


# Tests of the convert_paramstyle function.

def test_qmark():
    args = 1, 2, 3
    new_query, vals = convert(
        "qmark", "SELECT ?, ?, \"field_?\" FROM t "
        "WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'", args)
    expected = "SELECT $1, $2, \"field_?\" FROM t WHERE " \
        "a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'"
    assert (new_query, vals) == (expected, args)


def test_qmark_2():
    args = 1, 2, 3
    new_query, vals = convert(
        "qmark", "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'",
        args)
    expected = "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'"
    assert (new_query, vals) == (expected, args)


def test_numeric():
    args = 1, 2, 3
    new_query, vals = convert(
        "numeric",
        "SELECT sum(x)::decimal(5, 2) :2, :1, * FROM t WHERE a=:3", args)
    expected = "SELECT sum(x)::decimal(5, 2) $2, $1, * FROM t WHERE a=$3"
    assert (new_query, vals) == (expected, args)


def test_numeric_default_parameter():
    args = 1, 2, 3
    new_query, vals = convert("numeric", "make_interval(days := 10)", args)

    assert (new_query, vals) == ("make_interval(days := 10)", args)


def test_named():
    args = {
        "f_2": 1,
        "f1": 2,
    }
    new_query, vals = convert(
        "named",
        "SELECT sum(x)::decimal(5, 2) :f_2, :f1 FROM t WHERE a=:f_2", args)
    expected = "SELECT sum(x)::decimal(5, 2) $1, $2 FROM t WHERE a=$1"
    assert (new_query, vals) == (expected, (1, 2))


def test_format():
    args = 1, 2, 3
    new_query, vals = convert(
        "format", "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%s AND b='75%%' AND c = '%' -- Comment with %", args)
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " \
        "b='75%%' AND c = '%' -- Comment with %"
    assert (new_query, vals) == (expected, args)

    sql = r"""COMMENT ON TABLE test_schema.comment_test """ \
        r"""IS 'the test % '' " \ table comment'"""
    new_query, vals = convert("format", sql, args)
    assert (new_query, vals) == (sql, args)


def test_format_multiline():
    args = 1, 2, 3
    new_query, vals = convert("format", "SELECT -- Comment\n%s FROM t", args)
    assert (new_query, vals) == ("SELECT -- Comment\n$1 FROM t", args)


def test_py_format():
    args = {
        "f2": 1,
        "f1": 2,
        "f3": 3
    }

    new_query, vals = convert(
        "pyformat", "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%(f2)s AND b='75%%'", args)
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$1 AND " \
        "b='75%%'"
    assert (new_query, vals) == (expected, (1, 2))

    # pyformat should support %s and an array, too:
    args = 1, 2, 3
    new_query, vals = convert(
        "pyformat", "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%s AND b='75%%'", args)
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " \
        "b='75%%'"
    assert (new_query, vals) == (expected, args)
