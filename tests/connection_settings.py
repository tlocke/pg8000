from os import environ

db_connect = {
    'user': 'postgres',
    'password': 'pw'
}

try:
    db_connect['port'] = int(environ['PGPORT'])
except KeyError:
    pass
