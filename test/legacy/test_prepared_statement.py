def test_prepare(con):
    con.prepare("SELECT CAST(:v AS INTEGER)")


def test_run(con):
    ps = con.prepare("SELECT cast(:v as varchar)")
    ps.run(v="speedy")


def test_run_with_no_results(con):
    ps = con.prepare("ROLLBACK")
    ps.run()
