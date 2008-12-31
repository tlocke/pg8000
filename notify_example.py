import os
import select

from pg8000 import DBAPI

db = DBAPI.connect(host='localhost', user=os.environ['USER'], database='test')
cur = db.cursor()
# ABORT is equivalent to set_transaction_isolation(0) in psycopg2
cur.execute('ABORT')
cur.execute('LISTEN test')

while True:
    if select.select([cur], [], [], 5) == ([], [], []):
        print 'timeout'
    else:
        if cur.isready() and db.notifies:
            for backend_pid, notify in db.notifies:
                print '<%d> %s' % (backend_pid, notify)
            del db.notifies[:]
