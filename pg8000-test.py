#!/usr/bin/env python

import pg8000

db = pg8000.Connection(host='localhost', user='mfenniak')
db.iterate_dicts = True

cur1 = pg8000.Cursor(db)
cur2 = pg8000.Cursor(db)

cur1.execute("DROP TABLE t1")
cur1.execute("CREATE TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) not null)")
cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 1, 1, "hello")
cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 2, 10, u"he\u0173llo")
cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 3, 100, "hello")
cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 4, 1000, "hello")
cur1.execute("INSERT INTO t1 (f1, f2, f3) VALUES ($1, $2, $3)", 5, 10000, "hello")

print "begin query..."
cur1.execute("SELECT * FROM t1")
i = 0
for row1 in cur1:
    i = i + 1
    print i, repr(row1)
    cur2.execute("SELECT * FROM t1 WHERE f1 > $1", row1['f1'])
    for row2 in cur2:
        print "\t", repr(row2)
print "end query..."

print "begin query..."
cur1.execute("SELECT 5000 + 1 as int_test, True as bool_test, '2000-01-02 03:04:05.67'::timestamp as timestamp_test, 99999999999999999999::numeric")
for row in cur1:
    print repr(row)
print "end query..."

