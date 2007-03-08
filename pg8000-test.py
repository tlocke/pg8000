#!/usr/bin/env python

import datetime
import decimal

import pg8000

db = pg8000.Connection(host='joy.fenniak.net', user='Mathieu Fenniak', database="software", password="hello", socket_timeout=5)
db.iterate_dicts = True

cur1 = pg8000.Cursor(db)

#cur1.execute("DROP TABLE t1")
#cur1.execute("CREATE TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) not null)")
#cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 1, 1, "hello")
#cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 2, 10, u"he\u0173llo")
#cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 3, 100, "hello")
#cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 4, 1000, "hello")
#cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 5, 10000, "hello")

#print "begin query..."
#cur1.execute("SELECT * FROM t1")
#i = 0
#for row1 in cur1:
#    i = i + 1
#    print i, repr(row1)
#    db.execute("SELECT * FROM t1 WHERE f1 > $1", row1['f1'])
#    for row2 in db:
#        print "\t", repr(row2)
#print "end query..."

print "Beginning type checks..."

cur1.execute("SELECT 5000::smallint")
assert tuple(cur1) == ({"int2": 5000},)

cur1.execute("SELECT 5000::integer")
assert tuple(cur1) == ({"int4": 5000},)

cur1.execute("SELECT 50000000000000::bigint")
assert tuple(cur1) == ({"int8": 50000000000000},)

cur1.execute("SELECT 5000.023232::decimal")
assert tuple(cur1) == ({"numeric": decimal.Decimal("5000.023232")},)

cur1.execute("SELECT 1.1::real")
assert tuple(cur1) == ({"float4": 1.1000000000000001},)

cur1.execute("SELECT 1.1::double precision")
assert tuple(cur1) == ({"float8": 1.1000000000000001},)

cur1.execute("SELECT 'hello'::varchar(50)")
assert tuple(cur1) == ({"varchar": u"hello"},)

cur1.execute("SELECT 'hello'::char(20)")
assert tuple(cur1) == ({"bpchar": u"hello               "},)

cur1.execute("SELECT 'hello'::text")
assert tuple(cur1) == ({"text": u"hello"},)

#cur1.execute("SELECT 'hell\007o'::bytea")
#assert tuple(cur1) == ({"bytea": "hello"},)

cur1.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
assert tuple(cur1) == ({'timestamp': datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)},)

#cur1.execute("SELECT '2001-02-03 04:05:06.17'::timestamp with time zone")
#assert tuple(cur1) == ({'timestamp': datetime.datetime(2001, 2, 3, 4, 5, 6, 170000, pg8000.Types.FixedOffsetTz("-07"))},)

cur1.execute("SELECT '1 month'::interval")
assert tuple(cur1) == ({'interval': '1 mon'},)
#print repr(tuple(cur1))

print "Type checks complete."

