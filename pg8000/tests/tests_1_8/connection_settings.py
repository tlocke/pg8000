import os

'''
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
'''

NAME_VAR = "PG8000_TEST_NAME"
try:
    TEST_NAME = os.environ[NAME_VAR]
except KeyError:
    raise Exception(
        "The environment variable " + NAME_VAR + " needs to be set. It should "
        "contain the name of the environment variable that contains the "
        "kwargs for the connect() function.")

db_connect = eval(os.environ[TEST_NAME])
