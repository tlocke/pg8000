from io import BytesIO

import pytest


@pytest.fixture
def db_table(request, con):
    cursor = con.cursor()
    cursor.execute(
        "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
        "f2 int not null, f3 varchar(50) null) on commit drop"
    )
    return con


def test_copy_to_with_table(db_table):
    cursor = db_table.cursor()
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, 1))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 2, 2))
    cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 3, 3))

    stream = BytesIO()
    cursor.execute("copy t1 to stdout", stream=stream)
    assert stream.getvalue() == b"1\t1\t1\n2\t2\t2\n3\t3\t3\n"
    assert cursor.rowcount == 3


def test_copy_to_with_query(db_table):
    cursor = db_table.cursor()
    stream = BytesIO()
    cursor.execute(
        "COPY (SELECT 1 as One, 2 as Two) TO STDOUT WITH DELIMITER "
        "'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two",
        stream=stream,
    )
    assert stream.getvalue() == b"oneXtwo\n1XY2Y\n"
    assert cursor.rowcount == 1


def test_copy_from_with_table(db_table):
    cursor = db_table.cursor()
    stream = BytesIO(b"1\t1\t1\n2\t2\t2\n3\t3\t3\n")
    cursor.execute("copy t1 from STDIN", stream=stream)
    assert cursor.rowcount == 3

    cursor.execute("SELECT * FROM t1 ORDER BY f1")
    retval = cursor.fetchall()
    assert retval == ([1, 1, "1"], [2, 2, "2"], [3, 3, "3"])


def test_copy_from_with_query(db_table):
    cursor = db_table.cursor()
    stream = BytesIO(b"f1Xf2\n1XY1Y\n")
    cursor.execute(
        "COPY t1 (f1, f2) FROM STDIN WITH DELIMITER 'X' CSV HEADER "
        "QUOTE AS 'Y' FORCE NOT NULL f1",
        stream=stream,
    )
    assert cursor.rowcount == 1

    cursor.execute("SELECT * FROM t1 ORDER BY f1")
    retval = cursor.fetchall()
    assert retval == ([1, 1, None],)


def test_copy_from_with_error(db_table):
    cursor = db_table.cursor()
    stream = BytesIO(b"f1Xf2\n\n1XY1Y\n")
    with pytest.raises(BaseException) as e:
        cursor.execute(
            "COPY t1 (f1, f2) FROM STDIN WITH DELIMITER 'X' CSV HEADER "
            "QUOTE AS 'Y' FORCE NOT NULL f1",
            stream=stream,
        )

    arg = {
        "S": ("ERROR",),
        "C": ("22P02",),
        "M": (
            'invalid input syntax for type integer: ""',
            'invalid input syntax for integer: ""',
        ),
        "W": ('COPY t1, line 2, column f1: ""',),
        "F": ("numutils.c",),
        "R": ("pg_atoi", "pg_strtoint32"),
    }
    earg = e.value.args[0]
    for k, v in arg.items():
        assert earg[k] in v
