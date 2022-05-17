from io import BytesIO, StringIO

import pytest


@pytest.fixture
def db_table(request, con):
    con.run("START TRANSACTION")
    con.run(
        "CREATE TEMPORARY TABLE t1 "
        "(f1 int primary key, f2 int not null, f3 varchar(50) null) "
        "on commit drop"
    )
    return con


def test_copy_to_with_table(db_table):
    db_table.run("INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v1, :v2)", v1=1, v2="1")
    db_table.run("INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v1, :v2)", v1=2, v2="2")
    db_table.run("INSERT INTO t1 (f1, f2, f3) VALUES (:v1, :v1, :v2)", v1=3, v2="3")

    stream = BytesIO()
    db_table.run("copy t1 to stdout", stream=stream)
    assert stream.getvalue() == b"1\t1\t1\n2\t2\t2\n3\t3\t3\n"
    assert db_table.row_count == 3


def test_copy_to_with_query(con):
    stream = BytesIO()
    con.run(
        "COPY (SELECT 1 as One, 2 as Two) TO STDOUT WITH DELIMITER "
        "'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two",
        stream=stream,
    )
    assert stream.getvalue() == b"oneXtwo\n1XY2Y\n"
    assert con.row_count == 1


def test_copy_to_with_text_stream(con):
    stream = StringIO()
    con.run(
        "COPY (SELECT 1 as One, 2 as Two) TO STDOUT WITH DELIMITER "
        "'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two",
        stream=stream,
    )
    assert stream.getvalue() == "oneXtwo\n1XY2Y\n"
    assert con.row_count == 1


def test_copy_from_with_table(db_table):
    stream = BytesIO(b"1\t1\t1\n2\t2\t2\n3\t3\t3\n")
    db_table.run("copy t1 from STDIN", stream=stream)
    assert db_table.row_count == 3

    retval = db_table.run("SELECT * FROM t1 ORDER BY f1")
    assert retval == [[1, 1, "1"], [2, 2, "2"], [3, 3, "3"]]


def test_copy_from_with_text_stream(db_table):
    stream = StringIO("1\t1\t1\n2\t2\t2\n3\t3\t3\n")
    db_table.run("copy t1 from STDIN", stream=stream)

    retval = db_table.run("SELECT * FROM t1 ORDER BY f1")
    assert retval == [[1, 1, "1"], [2, 2, "2"], [3, 3, "3"]]


def test_copy_from_with_query(db_table):
    stream = BytesIO(b"f1Xf2\n1XY1Y\n")
    db_table.run(
        "COPY t1 (f1, f2) FROM STDIN WITH DELIMITER 'X' CSV HEADER "
        "QUOTE AS 'Y' FORCE NOT NULL f1",
        stream=stream,
    )
    assert db_table.row_count == 1

    retval = db_table.run("SELECT * FROM t1 ORDER BY f1")
    assert retval == [[1, 1, None]]


def test_copy_from_with_error(db_table):
    stream = BytesIO(b"f1Xf2\n\n1XY1Y\n")
    with pytest.raises(BaseException) as e:
        db_table.run(
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


def test_copy_from_with_text_iterable(db_table):
    stream = ["1\t1\t1\n", "2\t2\t2\n", "3\t3\t3\n"]
    db_table.run("copy t1 from STDIN", stream=stream)

    retval = db_table.run("SELECT * FROM t1 ORDER BY f1")
    assert retval == [[1, 1, "1"], [2, 2, "2"], [3, 3, "3"]]
