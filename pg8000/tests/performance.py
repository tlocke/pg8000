import pg8000
from pg8000.tests.connection_settings import db_connect
import time
import warnings
from contextlib import closing
from decimal import Decimal


whole_begin_time = time.time()

tests = (
        ("cast(id / 100 as int2)", 'int2'),
        ("cast(id as int4)", 'int4'),
        ("cast(id * 100 as int8)", 'int8'),
        ("(id %% 2) = 0", 'bool'),
        ("N'Static text string'", 'txt'),
        ("cast(id / 100 as float4)", 'float4'),
        ("cast(id / 100 as float8)", 'float8'),
        ("cast(id / 100 as numeric)", 'numeric'),
        ("timestamp '2001-09-28' + id * interval '1 second'", 'timestamp'),
)

with warnings.catch_warnings(), closing(pg8000.connect(**db_connect)) as db:
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
    cursor = db.cursor()
    cursor.execute(
        "CREATE TEMPORARY TABLE t1 (f1 serial primary key, "
        "f2 bigint not null, f3 varchar(50) null, f4 bool)")
    db.commit()
    params = [(Decimal('7.4009'), 'season of mists...', True)] * 1000
    print("Beginning executemany test...")
    for i in range(1, 5):
        begin_time = time.time()
        cursor.executemany(
            "insert into t1 (f2, f3, f4) values (%s, %s, %s)", params)
        db.commit()
        end_time = time.time()
        print("Attempt {0} took {1} seconds.".format(i, end_time - begin_time))

    print("Beginning reuse statements test...")
    begin_time = time.time()
    for i in range(2000):
        cursor.execute("select count(*) from t1")
        cursor.fetchall()
    print("Took {0} seconds.".format(time.time() - begin_time))

print("Whole time - %s seconds." % (time.time() - whole_begin_time))
