#!/usr/bin/env python

import datetime
import decimal
import threading

import pg8000

print "testing convert_paramstyle"

new_query, new_args = pg8000.DBAPI.convert_paramstyle("qmark", "SELECT ?, ?, \"field_?\" FROM t WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'", (1, 2, 3))
assert new_query == "SELECT $1, $2, \"field_?\" FROM t WHERE a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'"
assert new_args == (1, 2, 3)

new_query, new_args = pg8000.DBAPI.convert_paramstyle("qmark", "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'", (1, 2, 3))
assert new_query == "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'"
assert new_args == (1, 2, 3)

new_query, new_args = pg8000.DBAPI.convert_paramstyle("numeric", "SELECT :2, :1, * FROM t WHERE a=:3", (1, 2, 3))
assert new_query == "SELECT $2, $1, * FROM t WHERE a=$3"
assert new_args == (1, 2, 3)

new_query, new_args = pg8000.DBAPI.convert_paramstyle("named", "SELECT :f2, :f1 FROM t WHERE a=:f2", {"f2": 1, "f1": 2})
assert new_query == "SELECT $1, $2 FROM t WHERE a=$1"
assert new_args == (1, 2)

new_query, new_args = pg8000.DBAPI.convert_paramstyle("format", "SELECT %s, %s, \"f1_%%\", E'txt_%%' FROM t WHERE a=%s AND b='75%%'", (1, 2, 3))
assert new_query == "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$3 AND b='75%'"
assert new_args == (1, 2, 3)

new_query, new_args = pg8000.DBAPI.convert_paramstyle("pyformat", "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' FROM t WHERE a=%(f2)s AND b='75%%'", {"f2": 1, "f1": 2, "f3": 3})
assert new_query == "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$1 AND b='75%'"
assert new_args == (1, 2)

dbapi = pg8000.DBAPI

db = dbapi.connect(host='joy', user='pg8000-test', database='pg8000-test', password='pg8000-test', socket_timeout=5)
#db = dbapi.connect(host='localhost', user='mfenniak')
#db = dbapi.connect(user="mfenniak", unix_sock="/tmp/.s.PGSQL.5432")

cur = db.cursor()

print "creating test table"
cur.execute("DROP TABLE t1")
cur.execute("CREATE TABLE t1 (f1 int primary key, f2 int not null, f3 varchar(50) null)")
db.commit()

cur.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, 1))
cur.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, 10))

print "testing basic query..."
cur.execute("SELECT * FROM t1")
while 1:
    row = cur.fetchone()
    if row == None:
        break
    print repr(row)

print "tests complete"

