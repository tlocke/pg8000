#!/usr/bin/env python

import pg8000

db = pg8000.Connection(host='localhost', user='mfenniak')
cursor = pg8000.Cursor(db)
# db.iterate_dicts = True

print "begin query..."
cursor.execute("SELECT township, range, meridian FROM ats LIMIT 176")
i = 0
for row in cursor:
    i = i + 1
    print i, repr(row)
print "end query..."

print "begin query..."
cursor.execute("SELECT 5000 + 1, True as pg_stuff, False, '2000-01-02 03:04:05.67'::timestamp, $1", 55)
for row in cursor:
    print repr(row)
print "end query..."

