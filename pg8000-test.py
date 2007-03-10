#!/usr/bin/env python

import datetime
import decimal
import threading

import pg8000

#db = pg8000.Connection(host='joy', user='Mathieu Fenniak', database="software", password="hello", socket_timeout=5)
#db = pg8000.Connection(host='localhost', user='mfenniak')
db = pg8000.Connection(user="mfenniak", unix_sock="/tmp/.s.PGSQL.5432")

db.begin()

db.execute("DROP TABLE t1")
db.execute("CREATE TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")

# Not the most efficient way to do this.  Multiple DB connections would
# multiplex this insert and make it faster -- we're just testing for thread
# safety here.  Testing with much higher values of left/right allows
# multithread locking to be obvious.
s1 = pg8000.PreparedStatement(db, "INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", int, int, str)
def test(left, right):
    for i in range(left, right):
        s1.execute(i, id(threading.currentThread()), "test - unicode \u0173 ")
t1 = threading.Thread(target=test, args=(1, 10))
t2 = threading.Thread(target=test, args=(10, 20))
t3 = threading.Thread(target=test, args=(20, 30))
t4 = threading.Thread(target=test, args=(30, 40))
t1.start() ; t2.start() ; t3.start() ; t4.start()
t1.join(); t2.join(); t3.join(); t4.join()

db.commit()


print "begin query..."
db.execute("SELECT * FROM t1")
for row in db.iterate_dict():
    print repr(row)
print "end query..."

#print "begin query..."
cur1 = pg8000.Cursor(db)
#cur1.execute("SELECT * FROM t1")
#s1 = pg8000.PreparedStatement(db, "SELECT * FROM t1 WHERE f1 > $1", int)
#i = 0
#for row1 in cur1.iterate_dict():
#    i = i + 1
#    print i, repr(row1)
#    s1.execute(row1['f1'])
#    for row2 in s1.iterate_dict():
#        print "\t", repr(row2)
#print "end query..."

print "Beginning type checks..."

cur1.execute("SELECT $1", 5)
assert tuple(cur1.iterate_dict()) == ({"?column?": 5},)

cur1.execute("SELECT 5000::smallint")
assert tuple(cur1.iterate_dict()) == ({"int2": 5000},)

cur1.execute("SELECT 5000::integer")
assert tuple(cur1.iterate_dict()) == ({"int4": 5000},)

cur1.execute("SELECT 50000000000000::bigint")
assert tuple(cur1.iterate_dict()) == ({"int8": 50000000000000},)

cur1.execute("SELECT 5000.023232::decimal")
assert tuple(cur1.iterate_dict()) == ({"numeric": decimal.Decimal("5000.023232")},)

cur1.execute("SELECT 1.1::real")
assert tuple(cur1.iterate_dict()) == ({"float4": 1.1000000238418579},)

cur1.execute("SELECT 1.1::double precision")
assert tuple(cur1.iterate_dict()) == ({"float8": 1.1000000000000001},)

cur1.execute("SELECT 'hello'::varchar(50)")
assert tuple(cur1.iterate_dict()) == ({"varchar": u"hello"},)

cur1.execute("SELECT 'hello'::char(20)")
assert tuple(cur1.iterate_dict()) == ({"bpchar": u"hello               "},)

cur1.execute("SELECT 'hello'::text")
assert tuple(cur1.iterate_dict()) == ({"text": u"hello"},)

#cur1.execute("SELECT 'hell\007o'::bytea")
#assert tuple(cur1.iterate_dict()) == ({"bytea": "hello"},)

cur1.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
assert tuple(cur1.iterate_dict()) == ({'timestamp': datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)},)

#cur1.execute("SELECT '2001-02-03 04:05:06.17'::timestamp with time zone")
#assert tuple(cur1.iterate_dict()) == ({'timestamp': datetime.datetime(2001, 2, 3, 4, 5, 6, 170000, pg8000.Types.FixedOffsetTz("-07"))},)

cur1.execute("SELECT '1 month'::interval")
assert tuple(cur1.iterate_dict()) == ({'interval': '1 mon'},)
#print repr(tuple(cur1.iterate_dict()))

print "Type checks complete."

