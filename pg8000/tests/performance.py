from pg8000 import DBAPI
from .connection_settings import db_connect
import time
import warnings
from contextlib import closing
import decimal


tests = (
        ("(id / 100)::int2", 'int2'),
        ("id::int4", 'int4'),
        ("(id * 100)::int8", 'int8'),
        ("(id %% 2) = 0", 'bool'),
        ("N'Static text string'", 'txt'),
        ("id / 100::float4", 'float4'),
        ("id / 100::float8", 'float8'),
        ("id / 100::numeric", 'numeric'),
)

with warnings.catch_warnings(), closing(DBAPI.connect(**db_connect)) as db:
    warnings.simplefilter("ignore")
    for txt, name in tests:
        query = """SELECT {0} AS column1, {0} AS column2, {0} AS column3,
            {0} AS column4, {0} AS column5, {0} AS column6, {0} AS column7
            FROM (SELECT generate_series(1, 10000) AS id) AS tbl""".format(txt)
        cursor = db.cursor()
        print("Beginning %s test..." % name)
        for i in range(1, 5):
            begin_time = time.time()
            cursor.execute(query)
            for row in cursor:
                pass
            end_time = time.time()
            print("Attempt %s - %s seconds." % (i, end_time - begin_time))
    db.commit()
    cursor.execute(
        "CREATE TEMPORARY TABLE t1 (f1 serial primary key, "
        "f2 bigint not null, f3 varchar(50) null, f4 bool)")
    db.commit()
    params = [(decimal.Decimal('7.4009'), 'season of mists...', True) for i in range(1000)]
    print("Beginning executemany test...")
    for i in range(1, 5):
        begin_time = time.time()
        cursor.executemany(
            "insert into t1 (f2, f3, f4) values (%s, %s, %s)", params)
        db.commit()
        end_time = time.time()
        print("Attempt {0} took {1} seconds.".format(i, end_time - begin_time))



