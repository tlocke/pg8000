import socket
import struct

apilevel = '2.0'
threadsafety = 1
paramstyle = 'named' # check this later

class Warning(StandardError):
    pass

class Error(StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


class Cursor(object):
    def __init__(self, c):
        self.c = c
        self.arraysize = 1

    def getDescription(self):
        return None
    description = property(getDescription, None, None)

    def getRowCount(self):
        return -1
    rowcount = property(getRowCount, None, None)

    def callproc(self, procname, *params):
        pass

    def close(self):
        pass

    def execute(self, operation, *params):
        row_desc = self.c.query(operation)

    def executemany(self, operation, seq):
        pass

    def fetchone(self):
        row = self.c.getrow()
        return row

    def fetchmany(self, size=None):
        if size == None:
            size = self.arraysize
        pass

    def fetchall(self):
        retval = []
        while 1:
            row = self.fetchone()
            if row == None:
                break
            retval.append(row)
        return retval

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, *columns):
        pass


class Connection(object):
    def __init__(self, host, user, port=5432, database=None):
        try:
            self.c = Protocol.Connection(host, port)
            self.c.connect()
            self.c.authenticate(user)
        except socket.error, e:
            raise InterfaceError("socket.error", *e.args)
        self._cursor = Cursor(self.c)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return self._cursor

connect = Connection


class Protocol(object):
    class StartupMessage(object):
        def __init__(self, user, database=None):
            self.user = user
            self.database = database

        def serialize(self):
            protocol = 196608
            val = struct.pack("!i", protocol)
            val += "user\x00" + self.user + "\x00"
            if self.database:
                val += "database\x00" + self.database + "\x00"
            val += "\x00"
            val = struct.pack("!i", len(val) + 4) + val
            return val

    class Query(object):
        def __init__(self, qs):
            self.qs = qs

        def serialize(self):
            val = self.qs + "\x00"
            val = struct.pack("!i", len(val) + 4) + val
            val = "Q" + val
            return val

    class AuthenticationRequest(object):
        def createFromData(data):
            ident = struct.unpack("!i", data[:4])[0]
            if ident == 0:
                return Protocol.AuthenticationOk()
            else:
                return Protocol.AuthenticationRequest()
        createFromData = staticmethod(createFromData)

        def ok(self):
            return False

    class AuthenticationOk(AuthenticationRequest):
        def ok(self):
            return True

    class ParameterStatus(object):
        def __init__(self, key, value):
            self.key = key
            self.value = value

        def createFromData(data):
            key = data[:data.find("\x00")]
            value = data[data.find("\x00")+1:-1]
            return Protocol.ParameterStatus(key, value)
        createFromData = staticmethod(createFromData)

    class BackendKeyData(object):
        def __init__(self, process_id, secret_key):
            self.process_id = process_id
            self.secret_key = secret_key

        def createFromData(data):
            process_id, secret_key = struct.unpack("!2i", data)
            return Protocol.BackendKeyData(process_id, secret_key)
        createFromData = staticmethod(createFromData)

    class ReadyForQuery(object):
        def __init__(self, status):
            self.status = status

        def __repr__(self):
            return "<ReadyForQuery %s>" % \
                    {"I": "Idle", "T": "Idle in Transaction", "E": "Idle in Failed Transaction"}[self.status]

        def createFromData(data):
            return Protocol.ReadyForQuery(data)
        createFromData = staticmethod(createFromData)

    class ErrorResponse(object):
        def __init__(self, severity, code, msg):
            self.severity = severity
            self.code = code
            self.msg = msg

        def __repr__(self):
            return "<ErrorResponse %s %s %r>" % (self.severity, self.code, self.msg)

        def createFromData(data):
            args = {}
            for s in data.split("\x00"):
                if not s:
                    continue
                elif s[0] == "S":
                    args["severity"] = s[1:]
                elif s[0] == "C":
                    args["code"] = s[1:]
                elif s[0] == "M":
                    args["msg"] = s[1:]
            return ErrorResponse(**args)
        createFromData = staticmethod(createFromData)

    class RowDescription(object):
        def __init__(self, fields):
            self.fields = fields

        def createFromData(data):
            count = struct.unpack("!h", data[:2])[0]
            data = data[2:]
            fields = []
            for i in range(count):
                null = data.find("\x00")
                field = {"name": data[:null]}
                data = data[null+1:]
                field["table_oid"], field["column_attrnum"], field["type_oid"], field["type_size"], field["type_modifier"], field["format"] = struct.unpack("!ihihih", data[:18])
                data = data[19:]
                fields.append(field)
            return Protocol.RowDescription(fields)
        createFromData = staticmethod(createFromData)

    class CommandComplete(object):
        def __init__(self, tag):
            self.tag = tag

        def createFromData(data):
            return Protocol.CommandComplete(data[:-1])
        createFromData = staticmethod(createFromData)

    class DataRow(object):
        def __init__(self, fields):
            self.fields = fields

        def createFromData(data):
            count = struct.unpack("!h", data[:2])[0]
            data = data[2:]
            fields = []
            for i in range(count):
                val_len = struct.unpack("!i", data[:4])[0]
                data = data[4:]
                if val_len == -1:
                    fields.append(None)
                else:
                    fields.append(data[:val_len])
                    data = data[val_len + 1:]
            return Protocol.DataRow(fields)
        createFromData = staticmethod(createFromData)

    class Connection(object):
        def __init__(self, host=None, port=5432):
            self.state = "unconnected"
            self.host = host
            self.port = port
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        def verifyState(self, state):
            if self.state != state:
                raise ProgrammingError, "connection state must be %s, is %s" % (state, self.state)

        def _send(self, msg):
            self.sock.send(msg.serialize())

        def _read_message(self):
            bytes = self.sock.recv(5)
            assert len(bytes) == 5
            message_code = bytes[0]
            data_len = struct.unpack("!i", bytes[1:])[0] - 4
            bytes = self.sock.recv(data_len)
            return Protocol.message_types[message_code].createFromData(bytes)

        def connect(self):
            self.verifyState("unconnected")
            self.sock.connect((self.host, self.port))
            self.state = "noauth"

        def authenticate(self, user):
            self.verifyState("noauth")
            self._send(Protocol.StartupMessage(user))
            msg = self._read_message()
            if isinstance(msg, Protocol.AuthenticationOk):
                self.state = "auth"
                self._waitForReady()
                return True

        def _waitForReady(self):
            while 1:
                msg = self._read_message()
                if isinstance(msg, Protocol.ReadyForQuery):
                    self.state = "ready"
                    break

        def query(self, qs):
            self.verifyState("ready")
            self._send(Protocol.Query(qs))
            msg = self._read_message()
            if isinstance(msg, Protocol.RowDescription):
                self.state = "in_query"
                return msg

        def getrow(self):
            self.verifyState("in_query")
            msg = self._read_message()
            if isinstance(msg, Protocol.DataRow):
                return msg
            elif isinstance(msg, Protocol.CommandComplete):
                self.status = "query_complete"
                self._waitForReady()
                return None

    message_types = {
        "R": AuthenticationRequest,
        "S": ParameterStatus,
        "K": BackendKeyData,
        "Z": ReadyForQuery,
        "T": RowDescription,
        "E": ErrorResponse,
        "D": DataRow,
        "C": CommandComplete,
        }

