# vim: sw=4:expandtab:foldmethod=marker
#
# Copyright (c) 2007, Mathieu Fenniak
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"

import socket
import threading
import struct
import md5
from cStringIO import StringIO

from errors import *
import types

class SSLRequest(object):
    def __init__(self):
        pass

    def serialize(self):
        return struct.pack("!ii", 8, 80877103)

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

class Parse(object):
    def __init__(self, ps, qs, type_oids):
        self.ps = ps
        self.qs = qs
        self.type_oids = type_oids

    def serialize(self):
        val = self.ps + "\x00" + self.qs + "\x00"
        val = val + struct.pack("!h", len(self.type_oids))
        for oid in self.type_oids:
            # Parse message doesn't seem to handle the -1 type_oid for NULL
            # values that other messages handle.  So we'll provide type_oid 705,
            # the PG "unknown" type.
            if oid == -1: oid = 705
            val = val + struct.pack("!i", oid)
        val = struct.pack("!i", len(val) + 4) + val
        val = "P" + val
        return val

class Bind(object):
    def __init__(self, portal, ps, in_fc, params, out_fc, **kwargs):
        self.portal = portal
        self.ps = ps
        self.in_fc = in_fc
        self.params = []
        for i in range(len(params)):
            if len(self.in_fc) == 0:
                fc = 0
            elif len(self.in_fc) == 1:
                fc = self.in_fc[0]
            else:
                fc = self.in_fc[i]
            self.params.append(types.pg_value(params[i], fc, **kwargs))
        self.out_fc = out_fc

    def serialize(self):
        retval = StringIO()
        retval.write(self.portal + "\x00")
        retval.write(self.ps + "\x00")
        retval.write(struct.pack("!h", len(self.in_fc)))
        for fc in self.in_fc:
            retval.write(struct.pack("!h", fc))
        retval.write(struct.pack("!h", len(self.params)))
        for param in self.params:
            if param == None:
                # special case, NULL value
                retval.write(struct.pack("!i", -1))
            else:
                retval.write(struct.pack("!i", len(param)))
                retval.write(param)
        retval.write(struct.pack("!h", len(self.out_fc)))
        for fc in self.out_fc:
            retval.write(struct.pack("!h", fc))
        val = retval.getvalue()
        val = struct.pack("!i", len(val) + 4) + val
        val = "B" + val
        return val

class Close(object):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise InternalError("Close typ must be 1 char")
        self.typ = typ
        self.name = name

    def serialize(self):
        val = self.typ + self.name + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "C" + val
        return val

class ClosePortal(Close):
    def __init__(self, name):
        Close.__init__(self, "P", name)

class ClosePreparedStatement(Close):
    def __init__(self, name):
        Close.__init__(self, "S", name)

class Describe(object):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise InternalError("Describe typ must be 1 char")
        self.typ = typ
        self.name = name

    def serialize(self):
        val = self.typ + self.name + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "D" + val
        return val

class DescribePortal(Describe):
    def __init__(self, name):
        Describe.__init__(self, "P", name)

class DescribePreparedStatement(Describe):
    def __init__(self, name):
        Describe.__init__(self, "S", name)

class Flush(object):
    def serialize(self):
        return 'H\x00\x00\x00\x04'

class Sync(object):
    def serialize(self):
        return 'S\x00\x00\x00\x04'

class PasswordMessage(object):
    def __init__(self, pwd):
        self.pwd = pwd

    def serialize(self):
        val = self.pwd + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "p" + val
        return val

class Execute(object):
    def __init__(self, portal, row_count):
        self.portal = portal
        self.row_count = row_count

    def serialize(self):
        val = self.portal + "\x00" + struct.pack("!i", self.row_count)
        val = struct.pack("!i", len(val) + 4) + val
        val = "E" + val
        return val

class Terminate(object):
    def __init__(self):
        pass

    def serialize(self):
        return 'X\x00\x00\x00\x04'

class AuthenticationRequest(object):
    def __init__(self, data):
        pass

    def createFromData(data):
        ident = struct.unpack("!i", data[:4])[0]
        klass = authentication_codes.get(ident, None)
        if klass != None:
            return klass(data[4:])
        else:
            raise NotSupportedError("authentication method %r not supported" % (ident,))
    createFromData = staticmethod(createFromData)

    def ok(self, conn, user, **kwargs):
        raise InternalError("ok method should be overridden on AuthenticationRequest instance")

class AuthenticationOk(AuthenticationRequest):
    def ok(self, conn, user, **kwargs):
        return True

class AuthenticationMD5Password(AuthenticationRequest):
    def __init__(self, data):
        self.salt = "".join(struct.unpack("4c", data))

    def ok(self, conn, user, password=None, **kwargs):
        if password == None:
            raise InterfaceError("server requesting MD5 password authentication, but no password was provided")
        pwd = "md5" + md5.new(md5.new(password + user).hexdigest() + self.salt).hexdigest()
        conn._send(PasswordMessage(pwd))

        reader = MessageReader(conn)
        reader.add_message(AuthenticationRequest, lambda msg, reader: reader.return_value(msg.ok(conn, user)), reader)
        reader.add_message(ErrorResponse, self._ok_error)
        return reader.handle_messages()

    def _ok_error(self, msg):
        if msg.code == "28000":
            raise InterfaceError("md5 password authentication failed")
        else:
            raise msg.createException()

authentication_codes = {
    0: AuthenticationOk,
    5: AuthenticationMD5Password,
}

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

class NoData(object):
    def createFromData(data):
        return NoData()
    createFromData = staticmethod(createFromData)

class ParseComplete(object):
    def createFromData(data):
        return ParseComplete()
    createFromData = staticmethod(createFromData)

class BindComplete(object):
    def createFromData(data):
        return BindComplete()
    createFromData = staticmethod(createFromData)

class CloseComplete(object):
    def createFromData(data):
        return CloseComplete()
    createFromData = staticmethod(createFromData)

class PortalSuspended(object):
    def createFromData(data):
        return PortalSuspended()
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

class NoticeResponse(object):
    responseKeys = {
        "S": "severity",  # always present
        "C": "code",      # always present
        "M": "msg",       # always present
        "D": "detail",
        "H": "hint",
        "P": "position",
        "p": "_position",
        "q": "_query",
        "W": "where",
        "F": "file",
        "L": "line",
        "R": "routine",
    }

    def __init__(self, **kwargs):
        for arg, value in kwargs.items():
            setattr(self, arg, value)

    def __repr__(self):
        return "<NoticeResponse %s %s %r>" % (self.severity, self.code, self.msg)

    def dataIntoDict(data):
        retval = {}
        for s in data.split("\x00"):
            if not s: continue
            key, value = s[0], s[1:]
            key = NoticeResponse.responseKeys.get(key, key)
            retval[key] = value
        return retval
    dataIntoDict = staticmethod(dataIntoDict)

    def createFromData(data):
        return NoticeResponse(**NoticeResponse.dataIntoDict(data))
    createFromData = staticmethod(createFromData)

class ErrorResponse(object):
    def __init__(self, **kwargs):
        for arg, value in kwargs.items():
            setattr(self, arg, value)

    def __repr__(self):
        return "<ErrorResponse %s %s %r>" % (self.severity, self.code, self.msg)

    def createException(self):
        return ProgrammingError(self.severity, self.code, self.msg)

    def createFromData(data):
        return ErrorResponse(**NoticeResponse.dataIntoDict(data))
    createFromData = staticmethod(createFromData)

class NotificationResponse(object):
    # not implemented yet
    pass

class ParameterDescription(object):
    def __init__(self, type_oids):
        self.type_oids = type_oids
    def createFromData(data):
        count = struct.unpack("!h", data[:2])[0]
        type_oids = struct.unpack("!" + "i"*count, data[2:])
        return ParameterDescription(type_oids)
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
            data = data[18:]
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
                data = data[val_len:]
        return DataRow(fields)
    createFromData = staticmethod(createFromData)

class SSLWrapper(object):
    def __init__(self, sslobj):
        self.sslobj = sslobj
    def send(self, data):
        self.sslobj.write(data)
    def recv(self, num):
        return self.sslobj.read(num)

class MessageReader(object):
    def __init__(self, connection):
        self._conn = connection
        self._msgs = []

        # If true, raise exception from an ErrorResponse after messages are
        # processed.  This can be used to leave the connection in a usable
        # state after an error response, rather than having unconsumed
        # messages that won't be understood in another context.
        self.delay_raising_exception = False

    def add_message(self, msg_class, handler, *args, **kwargs):
        self._msgs.append((msg_class, handler, args, kwargs))

    def clear_messages(self):
        self._msgs = []

    def return_value(self, value):
        self._retval = value
    
    def handle_messages(self):
        exc = None
        while 1:
            msg = self._conn._read_message()
            msg_handled = False
            for (msg_class, handler, args, kwargs) in self._msgs:
                if isinstance(msg, msg_class):
                    msg_handled = True
                    retval = handler(msg, *args, **kwargs)
                    if retval:
                        # The handler returned a true value, meaning that the
                        # message loop should be aborted.
                        if exc != None:
                            raise exc
                        return retval
                    elif hasattr(self, "_retval"):
                        # The handler told us to return -- used for non-true
                        # return values
                        if exc != None:
                            raise exc
                        return self._retval
            if msg_handled:
                continue
            elif isinstance(msg, ErrorResponse):
                exc = msg.createException()
                if not self.delay_raising_exception:
                    raise exc
            elif isinstance(msg, NoticeResponse):
                # NoticeResponse can occur at any time, and must always be handled.
                self._conn.handleNoticeResponse(msg)
            elif isinstance(msg, ParameterStatus):
                # ParameterStatus can occur at any time, and must always be handled.
                self._conn.handleParameterStatus(msg)
            elif isinstance(msg, NotificationResponse):
                # NotificationResponse can occur at any time, and must always
                # be handled.  (Note, reading NotificationResponse isn't
                # handled yet.  This code block is kinda a comment to remember
                # to do it later.)
                self._conn.handleNotificationResponse(msg)
            else:
                raise InternalError("Unexpected response msg %r" % (msg))

class Connection(object):
    def __init__(self, unix_sock=None, host=None, port=5432, socket_timeout=60, ssl=False):
        self._client_encoding = "ascii"
        self._integer_datetimes = False
        if unix_sock == None and host != None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif unix_sock != None:
            if not hasattr(socket, "AF_UNIX"):
                raise InterfaceError("attempt to connect to unix socket on unsupported platform")
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            raise ProgrammingError("one of host or unix_sock must be provided")
        if unix_sock == None and host != None:
            self._sock.connect((host, port))
        elif unix_sock != None:
            self._sock.connect(unix_sock)
        if ssl:
            self._send(SSLRequest())
            resp = self._sock.recv(1)
            if resp == 'S':
                self._sock = SSLWrapper(socket.ssl(self._sock))
            else:
                raise InterfaceError("server refuses SSL")
        else:
            # settimeout causes ssl failure, on windows.  Python bug 1462352.
            self._sock.settimeout(socket_timeout)
        self._state = "noauth"
        self._backend_key_data = None
        self._sock_lock = threading.Lock()

    def verifyState(self, state):
        if self._state != state:
            raise InternalError("connection state must be %s, is %s" % (state, self._state))

    def _send(self, msg):
        data = msg.serialize()
        self._sock.send(data)

    def _read_message(self):
        bytes = ""
        while len(bytes) < 5:
            tmp = self._sock.recv(5 - len(bytes))
            bytes += tmp
        if len(bytes) != 5:
            raise InternalError("unable to read 5 bytes from socket %r" % bytes)
        message_code = bytes[0]
        data_len = struct.unpack("!i", bytes[1:])[0] - 4
        if data_len == 0:
            bytes = ""
        else:
            bytes = ""
            while len(bytes) < data_len:
                tmp = self._sock.recv(data_len - len(bytes))
                bytes += tmp
        assert len(bytes) == data_len
        msg = message_types[message_code].createFromData(bytes)
        return msg

    def authenticate(self, user, **kwargs):
        self.verifyState("noauth")
        self._sock_lock.acquire()
        try:
            self._send(StartupMessage(user, database=kwargs.get("database",None)))
            msg = self._read_message()
            if not isinstance(msg, AuthenticationRequest):
                raise InternalError("StartupMessage was responded to with non-AuthenticationRequest msg %r" % msg)
            if not msg.ok(self, user, **kwargs):
                raise InterfaceError("authentication method %s failed" % msg.__class__.__name__)

            self._state = "auth"

            reader = MessageReader(self)
            reader.add_message(ReadyForQuery, self._ready_for_query)
            reader.add_message(BackendKeyData, self._receive_backend_key_data)
            reader.handle_messages()
        finally:
            self._sock_lock.release()

    def _ready_for_query(self, msg):
        self._state = "ready"
        return True

    def _receive_backend_key_data(self, msg):
        self._backend_key_data = msg

    def parse(self, statement, qs, param_types):
        self.verifyState("ready")
        self._sock_lock.acquire()
        try:
            type_info = [types.pg_type_info(x) for x in param_types]
            param_types, param_fc = [x[0] for x in type_info], [x[1] for x in type_info] # zip(*type_info) -- fails on empty arr
            self._send(Parse(statement, qs, param_types))
            self._send(DescribePreparedStatement(statement))
            self._send(Flush())

            reader = MessageReader(self)

            # ParseComplete is good.
            reader.add_message(ParseComplete, lambda msg: 0)

            # Well, we don't really care -- we're going to send whatever we
            # want and let the database deal with it.  But thanks anyways!
            reader.add_message(ParameterDescription, lambda msg: 0)

            # We're not waiting for a row description.  Return something
            # destinctive to let bind know that there is no output.
            reader.add_message(NoData, lambda msg: (None, param_fc))

            # Common row description response
            reader.add_message(RowDescription, lambda msg: (msg, param_fc))

            return reader.handle_messages()

        finally:
            self._sock_lock.release()

    def bind(self, portal, statement, params, parse_data):
        self.verifyState("ready")
        self._sock_lock.acquire()
        try:
            row_desc, param_fc = parse_data
            if row_desc == None:
                # no data coming out
                output_fc = ()
            else:
                # We've got row_desc that allows us to identify what we're going to
                # get back from this statement.
                output_fc = [types.py_type_info(f) for f in row_desc.fields]
            self._send(Bind(portal, statement, param_fc, params, output_fc, client_encoding = self._client_encoding, integer_datetimes = self._integer_datetimes))
            # We need to describe the portal after bind, since the return
            # format codes will be different (hopefully, always what we
            # requested).
            self._send(DescribePortal(portal))
            self._send(Flush())

            # Read responses from server...
            reader = MessageReader(self)

            # BindComplete is good -- just ignore
            reader.add_message(BindComplete, lambda msg: 0)

            # NoData in this case means we're not executing a query.  As a
            # result, we won't be fetching rows, so we'll never execute the
            # portal we just created... unless we execute it right away, which
            # we'll do.
            reader.add_message(NoData, self._bind_nodata, portal, reader)

            # Return the new row desc, since it will have the format types we
            # asked the server for
            reader.add_message(RowDescription, lambda msg: msg)

            return reader.handle_messages()

        finally:
            self._sock_lock.release()

    def _bind_nodata(self, msg, portal, old_reader):
        # Bind message returned NoData, causing us to execute the command.
        self._send(Execute(portal, 0))
        self._send(Sync())

        reader = MessageReader(self)
        reader.add_message(CommandComplete, lambda msg: 0)
        reader.add_message(ReadyForQuery, lambda msg: 1)
        reader.delay_raising_exception = True
        reader.handle_messages()

        old_reader.return_value(None)

    def fetch_rows(self, portal, row_count, row_desc):
        self.verifyState("ready")
        self._sock_lock.acquire()
        try:
            self._send(Execute(portal, row_count))
            self._send(Flush())
            rows = []

            reader = MessageReader(self)
            reader.add_message(DataRow, self._fetch_datarow, rows, row_desc)
            reader.add_message(PortalSuspended, lambda msg: 1)
            reader.add_message(CommandComplete, self._fetch_commandcomplete, portal)
            retval = reader.handle_messages()

            # retval = 2 when command complete, indicating that we've hit the
            # end of the available data for this command
            return (retval == 2), rows
        finally:
            self._sock_lock.release()

    def _fetch_datarow(self, msg, rows, row_desc):
        rows.append(
                [types.py_value(msg.fields[i], row_desc.fields[i], client_encoding=self._client_encoding, integer_datetimes=self._integer_datetimes)
                    for i in range(len(msg.fields))]
                )

    def _fetch_commandcomplete(self, msg, portal):
        self._send(ClosePortal(portal))
        self._send(Sync())

        reader = MessageReader(self)
        reader.add_message(ReadyForQuery, self._fetch_commandcomplete_rfq)
        reader.add_message(CloseComplete, lambda msg: False)
        reader.handle_messages()

        return 2  # signal end-of-data

    def _fetch_commandcomplete_rfq(self, msg):
        self._state = "ready"
        return True

    def close_statement(self, statement):
        if self._state == "closed":
            return
        self.verifyState("ready")
        self._sock_lock.acquire()
        try:
            self._send(ClosePreparedStatement(statement))
            self._send(Sync())

            reader = MessageReader(self)
            reader.add_message(CloseComplete, lambda msg: 0)
            reader.add_message(ReadyForQuery, lambda msg: 1)
            reader.handle_messages()
        finally:
            self._sock_lock.release()

    def close_portal(self, portal):
        if self._state == "closed":
            return
        self.verifyState("ready")
        self._sock_lock.acquire()
        try:
            self._send(ClosePortal(portal))
            self._send(Sync())

            reader = MessageReader(self)
            reader.add_message(CloseComplete, lambda msg: 0)
            reader.add_message(ReadyForQuery, lambda msg: 1)
            reader.handle_messages()
        finally:
            self._sock_lock.release()

    def close(self):
        self._sock_lock.acquire()
        try:
            self._send(Terminate())
            self._sock.close()
            self._state = "closed"
        finally:
            self._sock_lock.release()

    def handleNoticeResponse(self, msg):
        # Note: this function will be called while things are going on.  Don't
        # monopolize the thread - do something quick, or spawn a thread,
        # because you'll be delaying whatever is going on.
        pass

    def handleParameterStatus(self, msg):
        # Note: this function will be called while things are going on.  Don't
        # monopolize the thread - do something quick, or spawn a thread,
        # because you'll be delaying whatever is going on.
        if msg.key == "client_encoding":
            self._client_encoding = msg.value
        elif msg.key == "integer_datetimes":
            self._integer_datetimes = (msg.value == "on")

    def handleNotificationResponse(self, msg):
        # Note: this function will be called while things are going on.  Don't
        # monopolize the thread - do something quick, or spawn a thread,
        # because you'll be delaying whatever is going on.
        pass

message_types = {
    "N": NoticeResponse,
    "R": AuthenticationRequest,
    "S": ParameterStatus,
    "K": BackendKeyData,
    "Z": ReadyForQuery,
    "T": RowDescription,
    "E": ErrorResponse,
    "D": DataRow,
    "C": CommandComplete,
    "1": ParseComplete,
    "2": BindComplete,
    "3": CloseComplete,
    "s": PortalSuspended,
    "n": NoData,
    "t": ParameterDescription,
    }


