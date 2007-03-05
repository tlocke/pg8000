#!/usr/bin/env python

import pg8000

db = pg8000.connect(host='localhost', user='mfenniak')
cur = db.cursor()

cur.execute("SELECT 1+1")
res = cur.fetchall()
print repr(res)

