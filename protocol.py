#!/usr/bin/env python

import socket
import struct

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
            return AuthenticationOk()
        else:
            return AuthenticationRequest()
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
        return ParameterStatus(key, value)
    createFromData = staticmethod(createFromData)

class BackendKeyData(object):
    def __init__(self, process_id, secret_key):
        self.process_id = process_id
        self.secret_key = secret_key

    def createFromData(data):
        process_id, secret_key = struct.unpack("!2i", data)
        return BackendKeyData(process_id, secret_key)
    createFromData = staticmethod(createFromData)

class ReadyForQuery(object):
    def __init__(self, status):
        self.status = status

    def __repr__(self):
        return "<ReadyForQuery %s>" % \
                {"I": "Idle", "T": "Idle in Transaction", "E": "Idle in Failed Transaction"}[self.status]

    def createFromData(data):
        return ReadyForQuery(data)
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
        return RowDescription(fields)
    createFromData = staticmethod(createFromData)

class CommandComplete(object):
    def __init__(self, tag):
        self.tag = tag

    def createFromData(data):
        return CommandComplete(data[:-1])
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
        return DataRow(fields)
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
        return message_types[message_code].createFromData(bytes)

    def connect(self):
        self.verifyState("unconnected")
        self.sock.connect((self.host, self.port))
        self.state = "noauth"

    def authenticate(self, user):
        self.verifyState("noauth")
        self._send(StartupMessage(user))
        msg = self._read_message()
        if isinstance(msg, AuthenticationOk):
            self.state = "auth"
            self._waitForReady()
            return True

    def _waitForReady(self):
        while 1:
            msg = self._read_message()
            if isinstance(msg, ReadyForQuery):
                self.state = "ready"
                break

    def query(self, qs):
        self.verifyState("ready")
        self._send(Query(qs))
        msg = self._read_message()
        if isinstance(msg, RowDescription):
            self.state = "in_query"
            return msg

    def getrow(self):
        self.verifyState("in_query")
        msg = self._read_message()
        if isinstance(msg, DataRow):
            return msg
        elif isinstance(msg, CommandComplete):
            self.status = "query_complete"
            self._waitForReady()
            return msg

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

#def read(sock):
#    ident = sock.recv(1)
#    size = struct.unpack("!i", sock.recv(4))[0] - 4
#    data = sock.recv(size)
#    message = message_types[ident].createFromData(data)
#    return message
#
#sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#sock.connect(("localhost", 5432))
#
#sock.send(StartupMessage("mfenniak").serialize())
#auth = read(sock)
#if not auth.ok():
#    print "auth failed"
#else:
#    while 1:
#        msg = read(sock)
#        print repr(msg)
#        if isinstance(msg, ReadyForQuery):
#            print "Ready for Query!"
#            break
#
##while 1:
##    val = sock.recv(1024)
##    if not len(val):
##        break
##    print repr(val)
#
#sock.close()

