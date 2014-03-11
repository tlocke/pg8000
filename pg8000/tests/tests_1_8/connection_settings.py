import os


db_stewart_connect = {
    "host": "127.0.0.1",
    "user": "pg8000-test",
    "database": "pg8000-test",
    "password": "pg8000-test",
    "socket_timeout": 5,
    "ssl": False}

db_local_connect = {
    "unix_sock": "/tmp/.s.PGSQL.5432",
    "user": "mfenniak"}

db_local_win_connect = {
    "host": "localhost",
    "user": "mfenniak",
    "password": "password",
    "database": "mfenniak"}

db_oracledev2_connect = {
    "host": "oracledev2",
    "user": "mfenniak",
    "password": "password",
    "database": "mfenniak"}

db_connect = eval(os.environ["PG8000_TEST"])
try:
    from testconfig import config
    try:
        db_connect['use_cache'] = config['use_cache'] == 'true'
    except KeyError:
        pass
except:
    # This means we're using Python 2.5 which is a special case.
    pass
