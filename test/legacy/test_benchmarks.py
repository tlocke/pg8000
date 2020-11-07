import pytest


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
    )
)
def test_round_trips(con, benchmark, txt):
    def torun():
        query = """SELECT {0} AS column1, {0} AS column2, {0} AS column3,
            {0} AS column4, {0} AS column5, {0} AS column6, {0} AS column7
            FROM (SELECT generate_series(1, 10000) AS id) AS tbl""".format(txt)
        con.run(query)
    benchmark(torun)
