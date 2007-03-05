#!/usr/bin/env python

print "Beginning unit tests."

test_count = 0
def begin_test(name):
    global test_count
    print "\t" * test_count,
    print name
    test_count += 1

def end_test():
    global test_count
    test_count -= 1


begin_test("import")
import protocol
end_test()

def test(db):
    global poll

    begin_test("connect")
    db.connect()
    end_test()

    begin_test("authenticate without password")
    auth = db.authenticate("mfenniak")
    assert auth
    end_test()

    begin_test("query1")
    row_desc = db.query("SELECT 1 + 1")
    begin_test("getrow")
    while 1:
        row = db.getrow()
        if isinstance(row, protocol.CommandComplete):
            break
        else:
            print repr(row), repr(row.fields)
    end_test()
    end_test()


begin_test("tcpip")
db = protocol.Connection(host="localhost")
test(db)
end_test()


