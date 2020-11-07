def test_prepare(con):
    con.prepare("SELECT :v")


def test_run(con):
    ps = con.prepare("SELECT cast(:v as varchar)")
    ps.run(v="speedy")
