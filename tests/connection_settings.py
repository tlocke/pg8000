db_joy_connect = {
        "host": "joy",
        "user": "pg8000-test",
        "database": "pg8000-test",
        "password": "pg8000-test",
        "socket_timeout": 5,
        "ssl": False,
        }

db_local_connect = {
        "unix_sock": "/tmp/.s.PGSQL.5432",
        "user": "mfenniak"
        }

db_local_win_connect = {
        "host": "localhost",
        "user": "mfenniak",
        "password": "password",
        "database": "mfenniak"
        }

db_connect = db_local_win_connect

