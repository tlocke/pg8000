import os.path
from os import environ

import pytest


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        'user': 'postgres',
        'password': 'pw'
    }

    for kw, var, f in [
                ('password', 'PGPASSWORD', str),
                ('port', 'PGPORT', int)
            ]:
        try:
            db_connect[kw] = f(environ[var])
        except KeyError:
            pass

    if 'PGHOST' in environ:
        pg_host = environ['PGHOST']
        if os.path.isabs(pg_host):
            db_connect['unix_sock'] = os.path.join(pg_host, '.s.PGSQL.{}'.format(db_connect.get('port', 5432)))
        else:
            db_connect['host'] = pg_host

    return db_connect
