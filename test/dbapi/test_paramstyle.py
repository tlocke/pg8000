import pytest

from pg8000.dbapi import convert_paramstyle

#        "(id %% 2) = 0",


@pytest.mark.parametrize(
    "query,statement",
    [
        [
            'SELECT ?, ?, "field_?" FROM t '
            "WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'",
            'SELECT $1, $2, "field_?" FROM t WHERE '
            "a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'",
        ],
        [
            "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'",
            "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'",
        ],
    ],
)
def test_qmark(query, statement):
    args = 1, 2, 3
    new_query, vals = convert_paramstyle("qmark", query, args)
    assert (new_query, vals) == (statement, args)


@pytest.mark.parametrize(
    "query,expected",
    [
        [
            "SELECT sum(x)::decimal(5, 2) :2, :1, * FROM t WHERE a=:3",
            "SELECT sum(x)::decimal(5, 2) $2, $1, * FROM t WHERE a=$3",
        ],
    ],
)
def test_numeric(query, expected):
    args = 1, 2, 3
    new_query, vals = convert_paramstyle("numeric", query, args)
    assert (new_query, vals) == (expected, args)


@pytest.mark.parametrize(
    "query",
    [
        "make_interval(days := 10)",
    ],
)
def test_numeric_unchanged(query):
    args = 1, 2, 3
    new_query, vals = convert_paramstyle("numeric", query, args)
    assert (new_query, vals) == (query, args)


def test_named():
    args = {
        "f_2": 1,
        "f1": 2,
    }
    new_query, vals = convert_paramstyle(
        "named", "SELECT sum(x)::decimal(5, 2) :f_2, :f1 FROM t WHERE a=:f_2", args
    )
    expected = "SELECT sum(x)::decimal(5, 2) $1, $2 FROM t WHERE a=$1"
    assert (new_query, vals) == (expected, (1, 2))


@pytest.mark.parametrize(
    "query,expected",
    [
        [
            "SELECT %s, %s, \"f1_%%\", E'txt_%%' "
            "FROM t WHERE a=%s AND b='75%%' AND c = '%' -- Comment with %",
            "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND "
            "b='75%%' AND c = '%' -- Comment with %",
        ],
        [
            "SELECT -- Comment\n%s FROM t",
            "SELECT -- Comment\n$1 FROM t",
        ],
    ],
)
def test_format_changed(query, expected):
    args = 1, 2, 3
    new_query, vals = convert_paramstyle("format", query, args)
    assert (new_query, vals) == (expected, args)


@pytest.mark.parametrize(
    "query",
    [
        r"""COMMENT ON TABLE test_schema.comment_test """
        r"""IS 'the test % '' " \ table comment'""",
    ],
)
def test_format_unchanged(query):
    args = 1, 2, 3
    new_query, vals = convert_paramstyle("format", query, args)
    assert (new_query, vals) == (query, args)


def test_py_format():
    args = {"f2": 1, "f1": 2, "f3": 3}

    new_query, vals = convert_paramstyle(
        "pyformat",
        "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' "
        "FROM t WHERE a=%(f2)s AND b='75%%'",
        args,
    )
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$1 AND " "b='75%%'"
    assert (new_query, vals) == (expected, (1, 2))


def test_pyformat_format():
    """pyformat should support %s and an array, too:"""
    args = 1, 2, 3
    new_query, vals = convert_paramstyle(
        "pyformat",
        "SELECT %s, %s, \"f1_%%\", E'txt_%%' " "FROM t WHERE a=%s AND b='75%%'",
        args,
    )
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " "b='75%%'"
    assert (new_query, vals) == (expected, args)
