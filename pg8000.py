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
import struct
import datetime
import md5
import decimal

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


class DataIterator(object):
    def __init__(self, connection):
        self.connection = connection
        if self.connection.iterate_dicts:
            self.method = Cursor.read_dict
        else:
            self.method = Cursor.read_tuple

    def __iter__(self):
        return self

    def next(self):
        retval = self.method(self.connection)
        if retval == None:
            raise StopIteration()
        return retval

class Cursor(object):
    ##
    # A configuration variable that determines whether iterating over the
    # connection will return tuples of queried rows (False), or dictionaries
    # indexed by column name/alias (True).  By default, this variable value is
    # copied from the connection's iterate_dicts value.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    iterate_dicts = False

    row_cache_size = 100

    def __init__(self, connection, name = None):
        self.iterate_dicts = connection.iterate_dicts
        self.c = connection.c
        if name == None:
            name = "pg8000_%s_%s" % (id(self.c), id(self))
        self.name = name
        self._row_desc = None
        self._cached_rows = []
        self._command_complete = True

    def execute(self, query, *args):
        self._cached_rows = []
        self._command_complete = False
        self._row_desc = self.c.extended_query(self.name, '', query, args)
        if self._row_desc == None:
            self._command_complete = True

    def _fetch(self):
        if not self._cached_rows:
            if self._command_complete:
                return None
            end_of_data, rows = self.c.fetch_rows(self.name, self.row_cache_size)
            self._cached_rows = rows
            if end_of_data:
                self._command_complete = True
                if not rows:
                    # special case - an empty query, hit end_of_data and no
                    # rows at the same time
                    return None
        row = self._cached_rows[0]
        del self._cached_rows[0]
        return tuple([Types.py_value(row.fields[i], self._row_desc.fields[i]) for i in range(len(row.fields))])

    ##
    # Read a row from the database server, and return it in a dictionary
    # indexed by column name/alias.  This method will raise an error if two
    # columns have the same name.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def read_dict(self):
        row = self._fetch()
        if row == None:
            return row
        retval = {}
        for i in range(len(self._row_desc.fields)):
            col_name = self._row_desc.fields[i]['name']
            if retval.has_key(col_name):
                raise InterfaceError("cannot return dict of row when two columns have the same name (%r)" % (col_name,))
            retval[col_name] = row[i]
        return retval

    ##
    # Read a row from the database server, and return it as a tuple of values.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def read_tuple(self):
        row = self._fetch()
        if row == None:
            return row
        return row

    ##
    # Iterate over query results.  The behaviour of iterating over this object
    # is dependent upon the value of the {@link #Connection.iterate_dicts
    # iterate_dicts} variable.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def __iter__(self):
        return DataIterator(self)

##
# This class represents a connection to a PostgreSQL database.
# <p>
# The database connection is derived from the {@link #Cursor Cursor} class, and
# provides access to the database's unnamed cursor through the standard Cursor
# methods.  It also provides transaction control via the 'begin', 'commit', and
# 'rollback' methods.  Without beginning a transaction explicitly, all
# statements will autocommit to the database.
# <p>
# Stability: Added in v1.00, stability guaranteed for v1.xx.
#
# @param host   The hostname of the PostgreSQL server to connect with.  Only
# TCP/IP connections are presently supported, so this parameter is mandatory.
#
# @param user   The username to connect to the PostgreSQL server with.  This
# parameter is mandatory.
#
# @keyparam port   The TCP/IP port of the PostgreSQL server instance.  This
# parameter defaults to 5432, the registered and common port of PostgreSQL
# TCP/IP servers.
#
# @keyparam database   The name of the database instance to connect with.  This
# parameter is optional, if omitted the PostgreSQL server will assume the
# database name is the same as the username.
#
# @keyparam password   The user password to connect to the server with.  This
# parameter is optional.  If omitted, and the database server requests password
# based authentication, the connection will fail.  On the other hand, if this
# parameter is provided and the database does not request password
# authentication, then the password will not be used.
class Connection(Cursor):

    ##
    # A configuration variable that determines whether iterating over the
    # connection will return tuples of queried rows (False), or dictionaries
    # indexed by column name/alias (True).  By default, this variable is set to
    # False.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    iterate_dicts = False

    def __init__(self, host, user, port=5432, database=None, password=None):
        self._row_desc = None
        try:
            self.c = Protocol.Connection(host, port)
            self.c.connect()
            self.c.authenticate(user, password=password, database=database)
        except socket.error, e:
            raise InterfaceError("communication error", e)
        Cursor.__init__(self, self, name='')

    def begin(self):
        raise NotSupportedError("uncoded")

    def commit(self):
        raise NotSupportedError("uncoded")

    def rollback(self):
        raise NotSupportedError("uncoded")


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

    class Parse(object):
        def __init__(self, ps, qs, types):
            self.ps = ps
            self.qs = qs
            self.types = [Types.pg_type_id(x) for x in types]

        def serialize(self):
            val = self.ps + "\x00" + self.qs + "\x00"
            val = val + struct.pack("!h", len(self.types))
            for oid in self.types:
                val = val + struct.pack("!i", oid)
            val = struct.pack("!i", len(val) + 4) + val
            val = "P" + val
            return val

    class Bind(object):
        def __init__(self, portal, ps, in_fc, params, out_fc, client_encoding):
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
                self.params.append(Types.pg_value(params[i], fc, client_encoding))
            self.out_fc = out_fc

        def serialize(self):
            val = self.portal + "\x00" + self.ps + "\x00"
            val = val + struct.pack("!h", len(self.in_fc))
            for fc in self.in_fc:
                val = val + struct.pack("!h", fc)
            val = val + struct.pack("!h", len(self.params))
            for param in self.params:
                val = val + struct.pack("!i", len(param)) + param
            val = val + struct.pack("!h", len(self.out_fc))
            for fc in self.out_fc:
                val = val + struct.pack("!h", fc)
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
            Protocol.Close.__init__(self, "P", name)

    class ClosePreparedStatement(Close):
        def __init__(self, name):
            Protocol.Close.__init__(self, "S", name)

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
            Protocol.Describe.__init__(self, "P", name)

    class DescribePreparedStatement(Describe):
        def __init__(self, name):
            Protocol.Describe.__init__(self, "S", name)

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

    class AuthenticationRequest(object):
        def __init__(self, data):
            pass

        def createFromData(data):
            ident = struct.unpack("!i", data[:4])[0]
            klass = Protocol.authentication_codes.get(ident, None)
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
            conn._send(Protocol.PasswordMessage(pwd))
            msg = conn._read_message()
            if isinstance(msg, Protocol.AuthenticationRequest):
                return msg.ok(conn, user)
            elif isinstance(msg, Protocol.ErrorResponse):
                if msg.code == "28000":
                    raise InterfaceError("md5 password authentication failed")
                else:
                    raise InternalError("server returned unexpected error %r" % msg)
            else:
                raise InternalError("server returned unexpected response %r" % msg)

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

    class NoData(object):
        def createFromData(data):
            return Protocol.NoData()
        createFromData = staticmethod(createFromData)

    class ParseComplete(object):
        def createFromData(data):
            return Protocol.ParseComplete()
        createFromData = staticmethod(createFromData)

    class BindComplete(object):
        def createFromData(data):
            return Protocol.BindComplete()
        createFromData = staticmethod(createFromData)

    class CloseComplete(object):
        def createFromData(data):
            return Protocol.CloseComplete()
        createFromData = staticmethod(createFromData)

    class PortalSuspended(object):
        def createFromData(data):
            return Protocol.PortalSuspended()
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

    class NoticeResponse(object):
        def __init__(self):
            pass
        def createFromData(data):
            # we could read the notice here, but we don't care yet.
            return Protocol.NoticeResponse()
        createFromData = staticmethod(createFromData)

    class ErrorResponse(object):
        def __init__(self, severity, code, msg):
            self.severity = severity
            self.code = code
            self.msg = msg

        def __repr__(self):
            return "<ErrorResponse %s %s %r>" % (self.severity, self.code, self.msg)

        def createException(self):
            return ProgrammingError(self.severity, self.code, self.msg)

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
            return Protocol.ErrorResponse(**args)
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
                    data = data[val_len:]
            return Protocol.DataRow(fields)
        createFromData = staticmethod(createFromData)

    class Connection(object):
        def __init__(self, host=None, port=5432):
            self._state = "unconnected"
            self._client_encoding = "ascii"
            self._host = host
            self._port = port
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._backend_key_data = None

        def verifyState(self, state):
            if self._state != state:
                raise InternalError("connection state must be %s, is %s" % (state, self._state))

        def _send(self, msg):
            self._sock.send(msg.serialize())

        def _read_message(self):
            bytes = self._sock.recv(5)
            assert len(bytes) == 5
            message_code = bytes[0]
            data_len = struct.unpack("!i", bytes[1:])[0] - 4
            bytes = self._sock.recv(data_len)
            msg = Protocol.message_types[message_code].createFromData(bytes)
            if isinstance(msg, Protocol.NoticeResponse):
                # ignore NoticeResponse
                return self._read_message()
            else:
                return msg

        def connect(self):
            self.verifyState("unconnected")
            self._sock.connect((self._host, self._port))
            self._state = "noauth"

        def authenticate(self, user, **kwargs):
            self.verifyState("noauth")
            self._send(Protocol.StartupMessage(user, database=kwargs.get("database",None)))
            msg = self._read_message()
            if isinstance(msg, Protocol.AuthenticationRequest):
                if msg.ok(self, user, **kwargs):
                    self._state = "auth"
                    while 1:
                        msg = self._read_message()
                        if isinstance(msg, Protocol.ReadyForQuery):
                            # done reading messages
                            self._state = "ready"
                            break
                        elif isinstance(msg, Protocol.ParameterStatus):
                            if msg.key == "client_encoding":
                                self._client_encoding = msg.value
                        elif isinstance(msg, Protocol.BackendKeyData):
                            self._backend_key_data = msg
                        elif isinstance(msg, Protocol.ErrorResponse):
                            raise msg.createException()
                        else:
                            raise InternalError("unexpected msg %r" % msg)
                else:
                    raise InterfaceError("authentication method %s failed" % msg.__class__.__name__)
            else:
                raise InternalError("StartupMessage was responded to with non-AuthenticationRequest msg %r" % msg)

        def extended_query(self, portal, statement, qs, params):
            self.verifyState("ready")
            self._send(Protocol.Parse(statement, qs, [type(x) for x in params]))
            self._send(Protocol.Bind(portal, statement, (0,), params, (0,), self._client_encoding))
            self._send(Protocol.DescribePortal(portal))
            self._send(Protocol.Flush())
            while 1:
                msg = self._read_message()
                if isinstance(msg, Protocol.ParseComplete):
                    # ok, good.
                    pass
                elif isinstance(msg, Protocol.BindComplete):
                    # good news everybody!
                    pass
                elif isinstance(msg, Protocol.NoData):
                    # no data means we should execute this command right away
                    self._send(Protocol.Execute(portal, 0))
                    self._send(Protocol.Sync())
                    while 1:
                        msg = self._read_message()
                        if isinstance(msg, Protocol.CommandComplete):
                            # more good news!
                            pass
                        elif isinstance(msg, Protocol.ReadyForQuery):
                            # ready to move on with life...
                            return None
                        elif isinstance(msg, Protocol.ErrorResponse):
                            raise msg.createException()
                        else:
                            raise InternalError("unexpected response")
                elif isinstance(msg, Protocol.RowDescription):
                    return msg
                elif isinstance(msg, Protocol.ErrorResponse):
                    raise msg.createException()
                else:
                    raise InternalError("Unexpected response msg %r" % (msg))

        def fetch_rows(self, portal, row_count):
            self.verifyState("ready")
            self._send(Protocol.Execute(portal, row_count))
            self._send(Protocol.Flush())
            rows = []
            end_of_data = False
            while 1:
                msg = self._read_message()
                if isinstance(msg, Protocol.DataRow):
                    rows.append(msg)
                elif isinstance(msg, Protocol.PortalSuspended):
                    # got all the rows we asked for, but not all that exist
                    break
                elif isinstance(msg, Protocol.CommandComplete):
                    self._send(Protocol.ClosePortal(portal))
                    self._send(Protocol.Sync())
                    while 1:
                        msg = self._read_message()
                        if isinstance(msg, Protocol.ReadyForQuery):
                            # ready to move on with life...
                            self._state = "ready"
                            break
                        elif isinstance(msg, Protocol.CloseComplete):
                            # ok, great!
                            pass
                        elif isinstance(msg, Protocol.ErrorResponse):
                            raise msg.createException()
                        else:
                            raise InternalError("unexpected response msg %r" % msg)
                    end_of_data = True
                    break
                elif isinstance(msg, Protocol.ErrorResponse):
                    raise msg.createException()
                else:
                    raise InternalError("Unexpected response msg %r" % msg)
            return end_of_data, rows

        def query(self, qs):
            self.verifyState("ready")
            self._send(Protocol.Query(qs))
            msg = self._read_message()
            if isinstance(msg, Protocol.RowDescription):
                self._state = "in_query"
                return msg
            elif isinstance(msg, Protocol.ErrorResponse):
                raise msg.createException()
            else:
                raise InternalError("RowDescription expected, other message recv'd")

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
        }

class Types(object):
    def pg_type_id(typ):
        data = Types.py_types.get(typ)
        if data == None:
            raise NotSupportedError("type %r not mapped to pg type" % typ)
        type_oid, func_txt, func_bin = data
        return type_oid
    pg_type_id = staticmethod(pg_type_id)

    def pg_value(v, fc, client_encoding):
        typ = type(v)
        data = Types.py_types.get(typ)
        if data == None:
            raise NotSupportedError("type %r not mapped to pg type" % typ)
        type_oid, func_txt, func_bin = data
        if fc == 0:
            func = func_txt
        else:
            func = func_bin
        if func == None:
            raise NotSupportedError("type %r, format code %r not converted" % (typ, fc))
        return func(v, client_encoding)
    pg_value = staticmethod(pg_value)

    def py_value(data, description):
        type_oid = description['type_oid']
        format = description['format']
        funcs = Types.pg_types.get(type_oid)
        if funcs == None:
            raise NotSupportedError("data response type %r not supported" % (type_oid))
        func = funcs[format]
        if func == None:
            raise NotSupportedError("data response format %r, type %r not supported" % (format, type_oid))
        return func(data, description)
    py_value = staticmethod(py_value)

    def boolin(data, description):
        return data == 't'

    def boolrecv(data, description):
        return data == "\x01"

    def int2recv(data, description):
        return struct.unpack("!h", data)[0]

    def int2in(data, description):
        return int(data)

    def int4recv(data, description):
        return struct.unpack("!i", data)[0]

    def int4in(data, description):
        return int(data)

    def int8in(data, description):
        return int(data)

    def float4in(data, description):
        return float(data)

    def float8in(data, description):
        return float(data)

    def timestamp_recv(data, description):
        val = struct.unpack("!d", data)[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds = val)

    def timestamp_in(data, description):
        year = int(data[0:4])
        month = int(data[5:7])
        day = int(data[8:10])
        hour = int(data[11:13])
        minute = int(data[14:16])
        sec = decimal.Decimal(data[17:])
        return datetime.datetime(year, month, day, hour, minute, int(sec), int((sec - int(sec)) * 1000000))

    def numeric_in(data, description):
        if data.find(".") == -1:
            return int(data)
        else:
            return decimal.Decimal(data)

    def numeric_out(v, ce):
        return str(v)

    def varcharin(data, description):
        return unicode(data, "utf-8")

    def textout(v, ce):
        return v.encode(ce)

    def timestamptz_in(data, description):
        year = int(data[0:4])
        month = int(data[5:7])
        day = int(data[8:10])
        hour = int(data[11:13])
        minute = int(data[14:16])
        tz_sep = data.rfind("-")
        sec = decimal.Decimal(data[17:tz_sep])
        tz = data[tz_sep:]
        print repr(data), repr(description)
        print repr(tz)
        return datetime.datetime(year, month, day, hour, minute, int(sec), int((sec - int(sec)) * 1000000), Types.FixedOffsetTz(tz))

    class FixedOffsetTz(datetime.tzinfo):
        def __init__(self, hrs):
            self.hrs = int(hrs)
            self.name = hrs

        def utcoffset(self, dt):
            return datetime.timedelta(hours=1) * self.hrs

        def tzname(self, dt):
            return self.name

        def dst(self, dt):
            return datetime.timedelta(0)

    # interval req. new patch for binary-output format prefered.
    #def interval_in(data, description):
    #    print repr(data), repr(description)

    py_types = {
        int: (1700, numeric_out, None),
        str: (25, textout, None),
        unicode: (25, textout, None),
    }

    pg_types = {
        16: (boolin, boolrecv),
        17: (None, None), # bytea not supported yet
        20: (int8in, None),
        21: (int2in, int2recv),
        23: (int4in, int4recv),
        25: (varcharin, None),  # text
        700: (float4in, None),
        701: (float8in, None),
        1042: (varcharin, None), # char
        1043: (varcharin, None), # varchar
        1114: (timestamp_in, timestamp_recv),
        1184: (timestamptz_in, None), # timestamp w/ tz
        1186: (interval_in, None),
        1700: (numeric_in, None),
    }

