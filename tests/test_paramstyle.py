from pg8000.core import convert_paramstyle as convert


# Tests of the convert_paramstyle function.

def test_qmark():
    new_query, make_args = convert(
        "qmark", "SELECT ?, ?, \"field_?\" FROM t "
        "WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'")
    expected = "SELECT $1, $2, \"field_?\" FROM t WHERE " \
        "a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'"
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_qmark_2():
    new_query, make_args = convert(
        "qmark", "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'")
    expected = "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'"
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_numeric():
    new_query, make_args = convert(
        "numeric",
        "SELECT sum(x)::decimal(5, 2) :2, :1, * FROM t WHERE a=:3")
    expected = "SELECT sum(x)::decimal(5, 2) $2, $1, * FROM t WHERE a=$3"
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_numeric_default_parameter():
    new_query, make_args = convert("numeric", "make_interval(days := 10)")

    assert new_query == "make_interval(days := 10)"
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_named():
    new_query, make_args = convert(
        "named",
        "SELECT sum(x)::decimal(5, 2) :f_2, :f1 FROM t WHERE a=:f_2")
    expected = "SELECT sum(x)::decimal(5, 2) $1, $2 FROM t WHERE a=$1"
    assert new_query == expected
    assert make_args({"f_2": 1, "f1": 2}) == (1, 2)


def test_format():
    new_query, make_args = convert(
        "format", "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%s AND b='75%%' AND c = '%' -- Comment with %")
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " \
        "b='75%%' AND c = '%' -- Comment with %"
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)

    sql = r"""COMMENT ON TABLE test_schema.comment_test """ \
        r"""IS 'the test % '' " \ table comment'"""
    new_query, make_args = convert("format", sql)
    assert new_query == sql


def test_format_multiline():
    new_query, make_args = convert("format", "SELECT -- Comment\n%s FROM t")
    assert new_query == "SELECT -- Comment\n$1 FROM t"


def test_py_format():
    new_query, make_args = convert(
        "pyformat", "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%(f2)s AND b='75%%'")
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$1 AND " \
        "b='75%%'"
    assert new_query == expected
    assert make_args({"f2": 1, "f1": 2, "f3": 3}) == (1, 2)

    # pyformat should support %s and an array, too:
    new_query, make_args = convert(
        "pyformat", "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%s AND b='75%%'")
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " \
        "b='75%%'"
    assert new_query, expected
    assert make_args((1, 2, 3)) == (1, 2, 3)
