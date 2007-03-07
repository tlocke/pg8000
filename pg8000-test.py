#!/usr/bin/env python

import pg8000

db = pg8000.Connection(host='localhost', user='mfenniak')
db.iterate_dicts = True

print "begin query..."
db.query("SELECT 5000 + 1, True as pg_stuff, False, '2000-01-02 03:04:05'::timestamp, $1", 55)
for row in db:
    print repr(row)
print "end query..."

