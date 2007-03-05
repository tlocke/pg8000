class PGconn(object):
    pass

def PQconnectdb(conninfo):
    conn = PQconnectStart(conninfo)
    if conn and conn.status != "CONNECTION_BAD":
        _connectDBComplete(conn)
    return conn

def PQconnectStart(conninfo):
    conn = PGconn()
    if not _connectOptions1(conn, conninfo):
        return conn
    if not _connectOptions2(conn):
        return conn
    if not _connectDBStart(conn):
        conn.status = "CONNECTION_BAD"
    return conn

def _connectOptions1(conn, conninfo):
    options = _conninfo_parse(conninfo)
    pass

def _connectOptions2(conn):
    pass

def _connectDBStart(conn):
    pass

def _conninfo_parse(conninfo):
    conninfo = conninfo + "\x00"
    i = 0
    while conninfo[i] != "\x00":
        cp = conninfo[i]


