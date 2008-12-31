import os
import select
from StringIO import StringIO

from pg8000 import DBAPI

db = DBAPI.connect(host='localhost', user=os.environ['USER'], database='test')
cur = db.cursor()
sio = StringIO()
cur.copy_to(sio, '(SELECT 1,2,3,4)')
assert sio.getvalue() == '1\t2\t3\t4\n'
cur.execute("SELECT 1,2,3,4")
assert cur.fetchone() == (1, 2, 3, 4)
cur.execute('CREATE TEMPORARY TABLE tmp_insert(a int, b int, c int, d int)')
sio.write('5\t6\t7\t8\n')
sio.seek(0)
cur.copy_from(sio, 'tmp_insert')
cur.execute("SELECT * FROM tmp_insert")
assert cur.fetchall() == ((1,2,3,4), (5,6,7,8))
