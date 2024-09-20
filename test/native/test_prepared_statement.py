def test_prepare(con):
    con.prepare("SELECT CAST(:v AS INTEGER)")


def test_run(con):
    ps = con.prepare("SELECT cast(:v as varchar)")
    ps.run(v="speedy")


def test_prepare_native(con):
    con.prepare("SELECT CAST($1 AS INTEGER)", native_params=True)


def test_run_native(con):
    ps = con.prepare("SELECT cast($1 as varchar)", native_params=True)
    rows = ps.run({1: "speedy"})
    assert rows[0] == ["speedy"]
