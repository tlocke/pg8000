#!/usr/bin/env python

import pg8000

db = pg8000.connect(host='localhost', user='mfenniak')
cur = db.cursor()

cur.execute("SELECT 5000+1, true, false, '2001-01-01 01:01:01'::timestamp")
res = cur.fetchall()
print repr(res)

