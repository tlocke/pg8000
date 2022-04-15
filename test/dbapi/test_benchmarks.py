import pytest

from pg8000.dbapi import connect


@pytest.mark.parametrize(
    "txt",
    (
        ("int2", "cast(id / 100 as int2)"),
        "cast(id as int4)",
        "cast(id * 100 as int8)",
        "(id % 2) = 0",
        "N'Static text string'",
        "cast(id / 100 as float4)",
        "cast(id / 100 as float8)",
        "cast(id / 100 as numeric)",
        "timestamp '2001-09-28'",
    ),
)
def test_round_trips(db_kwargs, benchmark, txt):
    def torun():
        with connect(**db_kwargs) as con:
            query = f"""SELECT {txt}, {txt}, {txt}, {txt}, {txt}, {txt}, {txt}
                FROM (SELECT generate_series(1, 10000) AS id) AS tbl"""
            cursor = con.cursor()
            cursor.execute(query)
            cursor.fetchall()
            cursor.close()

    benchmark(torun)
