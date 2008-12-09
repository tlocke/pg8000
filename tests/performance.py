from pg8000 import DBAPI
from .connection_settings import db_connect
import time

db = DBAPI.connect(**db_connect)

tests = (
        ("(id / 100)::int2", 'int2'),
        ("id::int4", 'int4'),
        ("(id * 100)::int8", 'int8'),
        ("(id %% 2) = 0", 'bool'),
        ("N'Static text string'", 'txt'),
        ("id / 100::float4", 'float4'),
        ("id / 100::float8", 'float8'),
)

for txt, name in tests:
    query = "SELECT %s AS %s_column FROM (SELECT generate_series(1, 10000) AS id) AS tbl" % (txt, name)
    cursor = db.cursor()
    print("Beginning %s test..." % name)
    for i in range(1, 5):
        print("Attempt %s" % i)
        begin_time = time.time()
        cursor.execute(query)
        for row in cursor:
            pass
        end_time = time.time()
        print("Attempt %s - %s seconds." % (i, end_time - begin_time))



# bytea
# numeric
# float4
# float8
# date
# time
# timestamp
# interval
