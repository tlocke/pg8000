# vim: sw=4:expandtab:foldmethod=marker
#
# Copyright (c) 2007-2009, Mathieu Fenniak
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
import ssl as sslmodule
import threading
from struct import unpack_from, pack
import hashlib
from pg8000.errors import CopyQueryWithoutStreamError, InterfaceError, \
    InternalError, ProgrammingError, NotSupportedError
from pg8000.util import MulticastDelegate
from pg8000 import types
from pg8000 import i_pack, i_unpack, h_pack, h_unpack, ii_pack, ii_unpack, \
    ihihih_unpack, ci_unpack, bh_unpack, cccc_unpack


##
# An SSLRequest message.  To initiate an SSL-encrypted connection, an
# SSLRequest message is used rather than a {@link StartupMessage
# StartupMessage}.  A StartupMessage is still sent, but only after SSL
# negotiation (if accepted).
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class SSLRequest(object):
    def __init__(self):
        pass

    # Int32(8) - Message length, including self.<br>
    # Int32(80877103) - The SSL request code.<br>
    def serialize(self):
        return ii_pack(8, 80877103)


##
# A StartupMessage message.  Begins a DB session, identifying the user to be
# authenticated as and the database to connect to.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class StartupMessage(object):
    def __init__(self, user, database=None):
        self.user = user
        self.database = database

    # Int32 - Message length, including self.
    # Int32(196608) - Protocol version number.  Version 3.0.
    # Any number of key/value pairs, terminated by a zero byte:
    #   String - A parameter name (user, database, or options)
    #   String - Parameter value
    def serialize(self):
        protocol = 196608
        val = bytearray(i_pack(protocol) + b"user\x00")
        val.extend(self.user.encode("ascii"))
        val.append(0)
        if self.database:
            val.extend(b"database\x00")
            val.extend(self.database.encode("ascii"))
            val.append(0)
        val.append(0)
        val = i_pack(len(val) + 4) + val
        return val


##
# Parse message.  Creates a prepared statement in the DB session.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param ps         Name of the prepared statement to create.
# @param qs         Query string.
# @param type_oids  An iterable that contains the PostgreSQL type OIDs for
#                   parameters in the query string.
class Parse(object):
    def __init__(self, ps, qs, type_oids):
        if isinstance(qs, str):
            raise TypeError("qs must be encoded byte data")
        self.ps = ps
        self.qs = qs
        self.type_oids = type_oids

    def __repr__(self):
        return "<Parse ps=%r qs=%r>" % (self.ps, self.qs)

    # Byte1('P') - Identifies the message as a Parse command.
    # Int32 -   Message length, including self.
    # String -  Prepared statement name.  An empty string selects the unnamed
    #           prepared statement.
    # String -  The query string.
    # Int16 -   Number of parameter data types specified (can be zero).
    # For each parameter:
    #   Int32 - The OID of the parameter data type.
    def serialize(self):
        val = bytearray(self.ps, "ascii")
        val.append(0)
        val.extend(self.qs)
        val.append(0)
        val.extend(h_pack(len(self.type_oids)))
        for oid in self.type_oids:
            # Parse message doesn't seem to handle the -1 type_oid for NULL
            # values that other messages handle.  So we'll provide type_oid
            # 705, the PG "unknown" type.
            if oid == -1:
                oid = 705
            val.extend(i_pack(oid))
        val[:0] = b"P" + i_pack(len(val) + 4)
        return val


##
# Bind message.  Readies a prepared statement for execution.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param portal     Name of the destination portal.
# @param ps         Name of the source prepared statement.
# @param in_fc      An iterable containing the format codes for input
#                   parameters.  0 = Text, 1 = Binary.
# @param params     The parameters.
# @param out_fc     An iterable containing the format codes for output
#                   parameters.  0 = Text, 1 = Binary.
# @param kwargs     Additional arguments to pass to the type conversion
#                   methods.
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
            value = types.pg_value(params[i], fc, **kwargs)
            if value is not None and not isinstance(value, bytes):
                raise InternalError(
                    "converting value %r to pgsql value returned non-bytes" %
                    params[i])
            self.params.append(value)
        self.out_fc = out_fc

    def __repr__(self):
        return "<Bind p=%r s=%r>" % (self.portal, self.ps)

    # Byte1('B') - Identifies the Bind command.
    # Int32 - Message length, including self.
    # String - Name of the destination portal.
    # String - Name of the source prepared statement.
    # Int16 - Number of parameter format codes.
    # For each parameter format code:
    #   Int16 - The parameter format code.
    # Int16 - Number of parameter values.
    # For each parameter value:
    #   Int32 - The length of the parameter value, in bytes, not including this
    #           this length.  -1 indicates a NULL parameter value, in which no
    #           value bytes follow.
    #   Byte[n] - Value of the parameter.
    # Int16 - The number of result-column format codes.
    # For each result-column format code:
    #   Int16 - The format code.
    def serialize(self):
        retval = bytearray(self.portal.encode("ascii") + b"\x00")
        retval.extend(self.ps.encode("ascii") + b"\x00")
        retval.extend(h_pack(len(self.in_fc)))
        retval.extend(pack("!" + "h" * len(self.in_fc), *self.in_fc))
        retval.extend(h_pack(len(self.params)))
        for param in self.params:
            if param is None:
                # special case, NULL value
                retval.extend(i_pack(-1))
            else:
                retval.extend(i_pack(len(param)))
                retval.extend(param)
        retval.extend(h_pack(len(self.out_fc)))
        retval.extend(pack("!" + "h" * len(self.out_fc), *self.out_fc))
        retval[:0] = b"B" + i_pack(len(retval) + 4)
        return retval


##
# A Close message, used for closing prepared statements and portals.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param typ    'S' for prepared statement, 'P' for portal.
# @param name   The name of the item to close.
class Close(object):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise InternalError("Close typ must be 1 char")
        self.typ = typ
        self.name = name

    # Byte1('C') - Identifies the message as a close command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def serialize(self):
        val = bytearray(self.typ + self.name.encode("ascii") + b"\x00")
        val[:0] = b"C" + i_pack(len(val) + 4)
        return val


##
# A specialized Close message for a portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ClosePortal(Close):
    def __init__(self, name):
        Close.__init__(self, b"P", name)


##
# A specialized Close message for a prepared statement.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ClosePreparedStatement(Close):
    def __init__(self, name):
        Close.__init__(self, b"S", name)


##
# A Describe message, used for obtaining information on prepared statements
# and portals.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param typ    'S' for prepared statement, 'P' for portal.
# @param name   The name of the item to close.
class Describe(object):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise InternalError("Describe typ must be 1 char")
        self.typ = typ
        self.name = name

    # Byte1('D') - Identifies the message as a describe command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def serialize(self):
        val = bytearray(self.typ + self.name.encode("ascii"))
        val.append(0)
        val[:0] = b"D" + i_pack(len(val) + 4)
        return val


##
# A specialized Describe message for a portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class DescribePortal(Describe):
    def __init__(self, name):
        Describe.__init__(self, b"P", name)

    def __repr__(self):
        return "<DescribePortal %r>" % (self.name)


##
# A specialized Describe message for a prepared statement.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class DescribePreparedStatement(Describe):
    def __init__(self, name):
        Describe.__init__(self, b"S", name)

    def __repr__(self):
        return "<DescribePreparedStatement %r>" % (self.name)


##
# A Flush message forces the backend to deliver any data pending in its
# output buffers.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Flush(object):
    # Byte1('H') - Identifies the message as a flush command.
    # Int32(4) - Length of message, including self.
    def serialize(self):
        return b'H\x00\x00\x00\x04'

    def __repr__(self):
        return "<Flush>"


##
# Causes the backend to close the current transaction (if not in a BEGIN/COMMIT
# block), and issue ReadyForQuery.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Sync(object):
    # Byte1('S') - Identifies the message as a sync command.
    # Int32(4) - Length of message, including self.
    def serialize(self):
        return b'S\x00\x00\x00\x04'

    def __repr__(self):
        return "<Sync>"


##
# Transmits a password.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class PasswordMessage(object):
    def __init__(self, pwd):
        self.pwd = pwd

    # Byte1('p') - Identifies the message as a password message.
    # Int32 - Message length including self.
    # String - The password.  Password may be encrypted.
    def serialize(self):
        val = bytearray(self.pwd)
        val.append(0)
        val[0:0] = b"p" + i_pack(len(val) + 4)
        return val


##
# Requests that the backend execute a portal and retrieve any number of rows.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
# @param row_count  The number of rows to return.  Can be zero to indicate the
#                   backend should return all rows. If the portal represents a
#                   query that does not return rows, no rows will be returned
#                   no matter what the row_count.
class Execute(object):
    def __init__(self, portal, row_count):
        self.portal = portal
        self.row_count = row_count

    # Byte1('E') - Identifies the message as an execute message.
    # Int32 -   Message length, including self.
    # String -  The name of the portal to execute.
    # Int32 -   Maximum number of rows to return, if portal contains a query
    # that returns rows.  0 = no limit.
    def serialize(self):
        val = bytearray(self.portal, "ascii")
        val.append(0)
        val.extend(i_pack(self.row_count))
        val[:0] = b"E" + i_pack(len(val) + 4)
        return val


##
# Informs the backend that the connection is being closed.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Terminate(object):
    def __init__(self):
        pass

    # Byte1('X') - Identifies the message as a terminate message.
    # Int32(4) - Message length, including self.
    def serialize(self):
        return b'X\x00\x00\x00\x04'


##
# Base class of all Authentication[*] messages.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class AuthenticationRequest(object):
    def __init__(self, data):
        pass

    # Byte1('R') - Identifies the message as an authentication request.
    # Int32(8) - Message length, including self.
    # Int32 -   An authentication code that represents different
    #           authentication messages:
    #               0 = AuthenticationOk
    #               5 = MD5 pwd
    #               2 = Kerberos v5 (not supported by pg8000)
    #               3 = Cleartext pwd (not supported by pg8000)
    #               4 = crypt() pwd (not supported by pg8000)
    #               6 = SCM credential (not supported by pg8000)
    #               7 = GSSAPI (not supported by pg8000)
    #               8 = GSSAPI data (not supported by pg8000)
    #               9 = SSPI (not supported by pg8000)
    # Some authentication messages have additional data following the
    # authentication code.  That data is documented in the appropriate class.
    @staticmethod
    def createFromData(data):
        try:
            return authentication_codes[i_unpack(data)[0]](data[4:])
        except KeyError as e:
            raise NotSupportedError(
                "authentication method {0} not supported".format(e))

    def ok(self, conn, user, **kwargs):
        raise InternalError(
            "ok method should be overridden on AuthenticationRequest instance")


##
# A message representing that the backend accepting the provided username
# without any challenge.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class AuthenticationOk(AuthenticationRequest):
    def ok(self, conn, user, **kwargs):
        return True


##
# A message representing the backend requesting an MD5 hashed password
# response.  The response will be sent as md5(md5(pwd + login) + salt).
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class AuthenticationMD5Password(AuthenticationRequest):
    # Additional message data:
    #  Byte4 - Hash salt.
    def __init__(self, data):
        self.salt = b"".join(cccc_unpack(data))

    def ok(self, conn, user, password=None, **kwargs):
        if password is None:
            raise InterfaceError(
                "server requesting MD5 password authentication, but no "
                "password was provided")
        pwd = b"md5" + hashlib.md5(
            hashlib.md5(
                password.encode("ascii") +
                user.encode("ascii")).hexdigest().encode("ascii") +
            self.salt).hexdigest().encode("ascii")
        conn._send(PasswordMessage(pwd))
        conn._flush()

        reader = MessageReader(conn)
        reader.add_message(
            AuthenticationRequest,
            lambda msg, reader: reader.return_value(msg.ok(conn, user)),
            reader)
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


##
# ParameterStatus message sent from backend, used to inform the frotnend of
# runtime configuration parameter changes.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ParameterStatus(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    # Byte1('S') - Identifies ParameterStatus
    # Int32 - Message length, including self.
    # String - Runtime parameter name.
    # String - Runtime parameter value.
    @classmethod
    def createFromData(cls, data):
        pos = data.find(b"\x00")
        return cls(data[:pos], data[pos + 1:-1])


##
# BackendKeyData message sent from backend.  Contains a connection's process
# ID and a secret key.  Can be used to terminate the connection's current
# actions, such as a long running query.  Not supported by pg8000 yet.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class BackendKeyData(object):
    def __init__(self, process_id, secret_key):
        self.process_id = process_id
        self.secret_key = secret_key

    # Byte1('K') - Identifier.
    # Int32(12) - Message length, including self.
    # Int32 - Process ID.
    # Int32 - Secret key.
    @classmethod
    def createFromData(cls, data):
        process_id, secret_key = ii_unpack(data)
        return cls(process_id, secret_key)


##
# Message representing a query with no data.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class NoData(object):
    # Byte1('n') - Identifier.
    # Int32(4) - Message length, including self.
    @classmethod
    def createFromData(cls, data):
        return cls()


##
# Message representing a successful Parse.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ParseComplete(object):
    # Byte1('1') - Identifier.
    # Int32(4) - Message length, including self.
    @classmethod
    def createFromData(cls, data):
        return cls()


##
# Message representing a successful Bind.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class BindComplete(object):
    # Byte1('2') - Identifier.
    # Int32(4) - Message length, including self.
    @classmethod
    def createFromData(cls, data):
        return cls()


##
# Message representing a successful Close.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class CloseComplete(object):
    # Byte1('3') - Identifier.
    # Int32(4) - Message length, including self.
    @classmethod
    def createFromData(cls, data):
        return cls()


##
# Message representing data from an Execute has been received, but more data
# exists in the portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class PortalSuspended(object):
    # Byte1('s') - Identifier.
    # Int32(4) - Message length, including self.
    @classmethod
    def createFromData(cls, data):
        return cls()


##
# Message representing the backend is ready to process a new query.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ReadyForQuery(object):
    def __init__(self, status):
        self._status = status

    ##
    # I = Idle, T = Idle in Transaction, E = idle in failed transaction.
    status = property(lambda self: self._status)

    def __repr__(self):
        return "<ReadyForQuery %s>" % {
            b"I": "Idle", b"T": "Idle in Transaction",
            b"E": "Idle in Failed Transaction"}[self.status]

    # Byte1('Z') - Identifier.
    # Int32(5) - Message length, including self.
    # Byte1 -   Status indicator.
    @classmethod
    def createFromData(cls, data):
        return cls(data)


##
# Represents a notice sent from the server.  This is not the same as a
# notification.  A notice is just additional information about a query, such
# as a notice that a primary key has automatically been created for a table.
# <p>
# A NoticeResponse instance will have properties containing the data sent
# from the server:
# <ul>
# <li>severity -- "ERROR", "FATAL', "PANIC", "WARNING", "NOTICE", "DEBUG",
# "INFO", or "LOG".  Always present.</li>
# <li>code -- the SQLSTATE code for the error.  See Appendix A of the
# PostgreSQL documentation for specific error codes.  Always present.</li>
# <li>msg -- human-readable error message.  Always present.</li>
# <li>detail -- Optional additional information.</li>
# <li>hint -- Optional suggestion about what to do about the issue.</li>
# <li>position -- Optional index into the query string.</li>
# <li>where -- Optional context.</li>
# <li>file -- Source-code file.</li>
# <li>line -- Source-code line.</li>
# <li>routine -- Source-code routine.</li>
# </ul>
# <p>
# Stability: Added in pg8000 v1.03.  Required properties severity, code, and
# msg are guaranteed for v1.xx.  Other properties should be checked with
# hasattr before accessing.
class NoticeResponse(object):
    responseKeys = {
        b"S": "severity",  # always present
        b"C": "code",      # always present
        b"M": "msg",       # always present
        b"D": "detail",
        b"H": "hint",
        b"P": "position",
        b"p": "_position",
        b"q": "_query",
        b"W": "where",
        b"F": "file",
        b"L": "line",
        b"R": "routine",
    }

    def __init__(self, **kwargs):
        for arg, value in list(kwargs.items()):
            setattr(self, arg, value)

    def __repr__(self):
        return "<NoticeResponse %s %s %r>" % (
            self.severity, self.code, self.msg)

    @staticmethod
    def dataIntoDict(data):
        retval = {}
        for s in data.split(b"\x00"):
            if not s:
                continue
            key, value = s[0:1], s[1:]
            key = NoticeResponse.responseKeys.get(key, key)
            retval[key] = value
        return retval

    # Byte1('N') - Identifier
    # Int32 - Message length
    # Any number of these, followed by a zero byte:
    #   Byte1 - code identifying the field type (see responseKeys)
    #   String - field value
    @classmethod
    def createFromData(cls, data):
        return cls(**NoticeResponse.dataIntoDict(data))


##
# A message sent in case of a server-side error.  Contains the same properties
# that {@link NoticeResponse NoticeResponse} contains.
# <p>
# Stability: Added in pg8000 v1.03.  Required properties severity, code, and
# msg are guaranteed for v1.xx.  Other properties should be checked with
# hasattr before accessing.
class ErrorResponse(object):
    def __init__(self, **kwargs):
        for arg, value in list(kwargs.items()):
            setattr(self, arg, value)

    def __repr__(self):
        return "<ErrorResponse %s %s %r>" % (
            self.severity, self.code, self.msg)

    def createException(self):
        return ProgrammingError(self.severity, self.code, self.msg)

    @classmethod
    def createFromData(cls, data):
        return cls(**NoticeResponse.dataIntoDict(data))


##
# A message sent if this connection receives a NOTIFY that it was LISTENing
# for.
# <p>
# Stability: Added in pg8000 v1.03.  When limited to accessing properties from
# a notification event dispatch, stability is guaranteed for v1.xx.
class NotificationResponse(object):
    def __init__(self, backend_pid, condition, additional_info):
        self._backend_pid = backend_pid
        self._condition = condition
        self._additional_info = additional_info

    ##
    # An integer representing the process ID of the backend that triggered
    # the NOTIFY.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    backend_pid = property(lambda self: self._backend_pid)

    ##
    # The name of the notification fired.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    condition = property(lambda self: self._condition)

    ##
    # Currently unspecified by the PostgreSQL documentation as of v8.3.1.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    additional_info = property(lambda self: self._additional_info)

    def __repr__(self):
        return "<NotificationResponse %s %s %r>" % (
            self.backend_pid, self.condition, self.additional_info)

    @classmethod
    def createFromData(cls, data):
        backend_pid = i_unpack(data)[0]
        idx = 4
        null = data.find(b"\x00", idx) - idx
        condition = data[idx:idx + null].decode("ascii")
        idx += null + 1
        null = data.find(b"\x00", idx) - idx
        additional_info = data[idx:idx + null]
        return cls(backend_pid, condition, additional_info)


class ParameterDescription(object):
    def __init__(self, type_oids):
        self.type_oids = type_oids

    @classmethod
    def createFromData(cls, data):
        count = h_unpack(data)[0]
        type_oids = unpack_from("!" + "i" * count, data, 2)
        return cls(type_oids)


class RowDescription(object):
    def __init__(self, fields):
        self.fields = fields

    @classmethod
    def createFromData(cls, data):
        count = h_unpack(data)[0]
        idx = 2
        fields = []
        for i in range(count):
            null = data.find(b"\x00", idx) - idx
            field = {"name": data[idx:idx + null]}
            idx += null + 1
            field["table_oid"], field["column_attrnum"], field["type_oid"], \
                field["type_size"], field["type_modifier"], field["format"] = \
                ihihih_unpack(data, idx)
            idx += 18
            fields.append(field)
        return cls(fields)


class CommandComplete(object):
    def __init__(self, command, rows=None, oid=None):
        self.command = command
        self.rows = rows
        self.oid = oid

    @classmethod
    def createFromData(cls, data):
        data = data[:-1]
        values = data.split(b" ")
        args = {}
        args['command'] = values[0]
        if args['command'] in (
                b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH", b"COPY"):
            args['rows'] = int(values[-1])
            if args['command'] == "INSERT":
                args['oid'] = int(values[1])
        else:
            args['command'] = data
        return cls(**args)


class DataRow(object):
    def __init__(self, fields):
        self.fields = fields

    @classmethod
    def createFromData(cls, data):
        count = h_unpack(data)[0]
        data_idx = 2
        fields = []
        for i in range(count):
            val_len = i_unpack(data, data_idx)[0]
            data_idx += 4
            if val_len == -1:
                fields.append(None)
            else:
                fields.append(data[data_idx:data_idx + val_len])
                data_idx += val_len
        return cls(fields)


class CopyData(object):
    # "d": CopyData,
    def __init__(self, data):
        self.data = data

    @classmethod
    def createFromData(cls, data):
        return cls(data)

    def serialize(self):
        return b'd' + i_pack(len(self.data) + 4) + self.data


class CopyDone(object):
    # Byte1('c') - Identifier.
    # Int32(4) - Message length, including self.

    @classmethod
    def createFromData(cls, data):
        return cls()

    def serialize(self):
        return b'c\x00\x00\x00\x04'


class CopyOutResponse(object):
    # Byte1('H')
    # Int32(4) - Length of message contents in bytes, including self.
    # Int8(1) - 0 textual, 1 binary
    # Int16(2) - Number of columns
    # Int16(N) - Format codes for each column (0 text, 1 binary)

    def __init__(self, is_binary, column_formats):
        self.is_binary = is_binary
        self.column_formats = column_formats

    @classmethod
    def createFromData(cls, data):
        is_binary, num_cols = bh_unpack(data)
        column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        return cls(is_binary, column_formats)


class CopyInResponse(object):
    # Byte1('G')
    # Otherwise the same as CopyOutResponse

    def __init__(self, is_binary, column_formats):
        self.is_binary = is_binary
        self.column_formats = column_formats

    @classmethod
    def createFromData(cls, data):
        is_binary, num_cols = bh_unpack(data)
        column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        return cls(is_binary, column_formats)


class MessageReader(object):
    def __init__(self, connection):
        self._conn = connection
        self._msgs = []

        # If true, raise exception from an ErrorResponse after messages are
        # processed.  This can be used to leave the connection in a usable
        # state after an error response, rather than having unconsumed
        # messages that won't be understood in another context.
        self.delay_raising_exception = False

        self.ignore_unhandled_messages = False

    def add_message(self, msg_class, handler, *args, **kwargs):
        self._msgs.append((msg_class, handler, args, kwargs))

    def clear_messages(self):
        self._msgs = []

    def return_value(self, value):
        self._retval = value

    def handle_messages(self):
        exc = None
        read_bytes = self._conn._sock.read
        while True:
            assert self._conn._sock_lock.locked()
            message_code, data_len = ci_unpack(read_bytes(5))
            msg = message_types[
                message_code].createFromData(read_bytes(data_len - 4))

            msg_handled = False
            for msg_class, handler, args, kwargs in self._msgs:
                if isinstance(msg, msg_class):
                    msg_handled = True
                    retval = handler(msg, *args, **kwargs)
                    if retval:
                        # The handler returned a true value, meaning that the
                        # message loop should be aborted.
                        if exc is not None:
                            raise exc
                        return retval
                    elif hasattr(self, "_retval"):
                        # The handler told us to return -- used for non-true
                        # return values
                        if exc is not None:
                            raise exc
                        return self._retval
            if msg_handled:
                continue
            elif isinstance(msg, ErrorResponse):
                exc = msg.createException()
                if not self.delay_raising_exception:
                    raise exc
            elif isinstance(msg, NoticeResponse):
                self._conn.handleNoticeResponse(msg)
            elif isinstance(msg, ParameterStatus):
                self._conn.handleParameterStatus(msg)
            elif isinstance(msg, NotificationResponse):
                self._conn.handleNotificationResponse(msg)
            elif not self.ignore_unhandled_messages:
                raise InternalError("Unexpected response msg %r" % (msg))


def sync_on_error(fn):
    def _fn(self, *args, **kwargs):
        with self._sock_lock:
            try:
                return fn(self, *args, **kwargs)
            except:
                try:
                    self._sync()
                finally:
                    raise
    return _fn


class Connection(object):
    def __init__(
            self, unix_sock=None, host=None, port=5432, socket_timeout=60,
            ssl=False):
        self._client_encoding = "ascii"
        self._integer_datetimes = False
        self._sock_lock = threading.Lock()
        if unix_sock is None and host is not None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif unix_sock is not None:
            if not hasattr(socket, "AF_UNIX"):
                raise InterfaceError(
                    "attempt to connect to unix socket on unsupported "
                    "platform")
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            raise ProgrammingError("one of host or unix_sock must be provided")
        if unix_sock is None and host is not None:
            self._sock.connect((host, port))
        elif unix_sock is not None:
            self._sock.connect(unix_sock)
        if ssl:
            with self._sock_lock:
                self._send(SSLRequest())
                self._flush()
                resp = self._sock.recv(1)
                if resp == 'S':
                    self._sock = sslmodule.wrap_socket(self._sock)
                else:
                    raise InterfaceError("server refuses SSL")
        else:
            # settimeout causes ssl failure, on windows.  Python bug 1462352.
            self._sock.settimeout(socket_timeout)

        self._sock = self._sock.makefile(mode="rwb", buffering=1024)
        self._state = "noauth"
        self._backend_key_data = None

        self.NoticeReceived = MulticastDelegate()
        self.ParameterStatusReceived = MulticastDelegate()
        self.NotificationReceived = MulticastDelegate()

        self.ParameterStatusReceived += self._onParameterStatusReceived

    def verifyState(self, state):
        if self._state != state:
            raise InternalError(
                "connection state must be %s, is %s" % (state, self._state))

    def _send(self, msg):
        assert self._sock_lock.locked()
        self._sock.write(msg.serialize())

    def _flush(self):
        assert self._sock_lock.locked()
        self._sock.flush()

    def authenticate(self, user, **kwargs):
        self.verifyState("noauth")
        with self._sock_lock:
            self._send(
                StartupMessage(user, database=kwargs.get("database", None)))
            self._flush()

            reader = MessageReader(self)
            reader.add_message(
                AuthenticationRequest,
                self._authentication_request(user, **kwargs))
            reader.handle_messages()

    def _authentication_request(self, user, **kwargs):
        def _func(msg):
            assert self._sock_lock.locked()
            if not msg.ok(self, user, **kwargs):
                raise InterfaceError(
                    "authentication method %s failed" % msg.__class__.__name__)
            self._state = "auth"
            reader = MessageReader(self)
            reader.add_message(ReadyForQuery, self._ready_for_query)
            reader.add_message(BackendKeyData, self._receive_backend_key_data)
            reader.handle_messages()
            return 1
        return _func

    def _ready_for_query(self, msg):
        self._state = "ready"
        return True

    def _receive_backend_key_data(self, msg):
        self._backend_key_data = msg

    @sync_on_error
    def parse(self, statement, qs, param_types):
        self.verifyState("ready")

        type_info = [types.pg_type_info(x) for x in param_types]
        param_types, param_fc = [x[0] for x in type_info], \
            [x[1] for x in type_info]  # zip(*type_info) -- fails on empty arr
        self._send(
            Parse(statement, qs.encode(self._client_encoding), param_types))
        self._send(DescribePreparedStatement(statement))
        self._send(Flush())
        self._flush()

        reader = MessageReader(self)

        # ParseComplete is good.
        reader.add_message(ParseComplete, lambda msg: False)

        # Well, we don't really care -- we're going to send whatever we
        # want and let the database deal with it.  But thanks anyways!
        reader.add_message(ParameterDescription, lambda msg: False)

        # We're not waiting for a row description.  Return something
        # destinctive to let bind know that there is no output.
        reader.add_message(NoData, lambda msg: (None, param_fc))

        # Common row description response
        reader.add_message(RowDescription, lambda msg: (msg, param_fc))

        return reader.handle_messages()

    @sync_on_error
    def bind(self, portal, statement, params, parse_data, copy_stream):
        self.verifyState("ready")

        row_desc, param_fc = parse_data
        if row_desc is None:
            # no data coming out
            output_fc = ()
        else:
            # We've got row_desc that allows us to identify what we're going to
            # get back from this statement.
            try:
                output_fc = tuple(
                    types.pg_types[f['type_oid']][0] for f in row_desc.fields)
            except KeyError as e:
                raise NotSupportedError(
                    "type oid %r not mapped to py type" % str(e))
        self._send(
            Bind(
                portal, statement, param_fc, params, output_fc,
                client_encoding=self._client_encoding,
                integer_datetimes=self._integer_datetimes))
        # We need to describe the portal after bind, since the return
        # format codes will be different (hopefully, always what we
        # requested).
        self._send(DescribePortal(portal))
        self._send(Flush())
        self._flush()

        # Read responses from server...
        reader = MessageReader(self)

        # BindComplete is good -- just ignore
        reader.add_message(BindComplete, lambda msg: False)

        # NoData in this case means we're not executing a query.  As a
        # result, we won't be fetching rows, so we'll never execute the
        # portal we just created... unless we execute it right away, which
        # we'll do.
        reader.add_message(
            NoData, self._bind_nodata, portal, reader, copy_stream)

        # Return the new row desc, since it will have the format types we
        # asked the server for
        reader.add_message(RowDescription, lambda msg: (msg, None))

        return reader.handle_messages()

    def _copy_in_response(self, copyin, fileobj, old_reader):
        if fileobj is None:
            raise CopyQueryWithoutStreamError()
        while True:
            data = fileobj.read(8192)
            if not data:
                break
            self._send(CopyData(data))
            self._flush()
        self._send(CopyDone())
        self._send(Sync())
        self._flush()

    def _copy_out_response(self, copyout, fileobj, old_reader):
        if fileobj is None:
            raise CopyQueryWithoutStreamError()
        reader = MessageReader(self)
        reader.add_message(CopyData, self._copy_data, fileobj)
        reader.add_message(CopyDone, lambda msg: 1)
        reader.handle_messages()

    def _copy_data(self, copydata, fileobj):
        fileobj.write(copydata.data)

    def _bind_nodata(self, msg, portal, old_reader, copy_stream):
        # Bind message returned NoData, causing us to execute the command.
        self._send(Execute(portal, 0))
        self._send(Sync())
        self._flush()

        output = {}
        reader = MessageReader(self)
        reader.add_message(
            CopyOutResponse, self._copy_out_response, copy_stream, reader)
        reader.add_message(
            CopyInResponse, self._copy_in_response, copy_stream, reader)
        reader.add_message(
            CommandComplete,
            lambda msg, out: out.setdefault('msg', msg) and False, output)
        reader.add_message(ReadyForQuery, lambda msg: True)
        reader.delay_raising_exception = True
        reader.handle_messages()

        old_reader.return_value((None, output['msg']))

    @sync_on_error
    def fetch_rows(self, portal, row_count, row_desc):
        self.verifyState("ready")

        self._send(Execute(portal, row_count))
        self._send(Flush())
        self._flush()
        rows = []

        reader = MessageReader(self)
        reader.add_message(DataRow, self._fetch_datarow, rows, row_desc)
        reader.add_message(PortalSuspended, lambda msg: True)
        reader.add_message(
            CommandComplete, self._fetch_commandcomplete, portal)
        retval = reader.handle_messages()

        # retval = 2 when command complete, indicating that we've hit the
        # end of the available data for this command
        return (retval == 2), rows

    def _fetch_datarow(self, msg, rows, row_desc):
        rows.append(
            [types.py_value(
                msg.fields[i], row_desc.fields[i],
                client_encoding=self._client_encoding,
                integer_datetimes=self._integer_datetimes,)
                for i in range(len(msg.fields))])

    def _fetch_commandcomplete(self, msg, portal):
        self._send(ClosePortal(portal))
        self._send(Sync())
        self._flush()

        reader = MessageReader(self)
        reader.add_message(ReadyForQuery, self._fetch_commandcomplete_rfq)
        reader.add_message(CloseComplete, lambda msg: False)
        reader.handle_messages()

        return 2  # signal end-of-data

    def _fetch_commandcomplete_rfq(self, msg):
        self._state = "ready"
        return True

    # Send a Sync message, then read and discard all messages until we
    # receive a ReadyForQuery message.
    def _sync(self):
        # it is assumed _sync is called from sync_on_error, which holds
        # a _sock_lock throughout the call
        self._send(Sync())
        self._flush()
        reader = MessageReader(self)
        reader.ignore_unhandled_messages = True
        reader.add_message(ReadyForQuery, lambda msg: True)
        reader.handle_messages()

    def close_statement(self, statement):
        if self._state == "closed":
            return
        self.verifyState("ready")

        with self._sock_lock:
            self._send(ClosePreparedStatement(statement))
            self._send(Sync())
            self._flush()

            reader = MessageReader(self)
            reader.add_message(CloseComplete, lambda msg: False)
            reader.add_message(ReadyForQuery, lambda msg: True)
            reader.handle_messages()

    def close_portal(self, portal):
        if self._state == "closed":
            return
        self.verifyState("ready")
        with self._sock_lock:
            self._send(ClosePortal(portal))
            self._send(Sync())
            self._flush()

            reader = MessageReader(self)
            reader.add_message(CloseComplete, lambda msg: False)
            reader.add_message(ReadyForQuery, lambda msg: True)
            reader.handle_messages()

    def close(self):
        with self._sock_lock:
            self._send(Terminate())
            self._flush()
            self._sock.close()
            self._state = "closed"

    def _onParameterStatusReceived(self, msg):
        if msg.key == b"client_encoding":
            self._client_encoding = types.encoding_convert(msg.value)
        elif msg.key == b"integer_datetimes":
            self._integer_datetimes = (msg.value == b"on")

    def handleNoticeResponse(self, msg):
        self.NoticeReceived(msg)

    def handleParameterStatus(self, msg):
        self.ParameterStatusReceived(msg)

    def handleNotificationResponse(self, msg):
        self.NotificationReceived(msg)


message_types = {
    b"N": NoticeResponse,
    b"R": AuthenticationRequest,
    b"S": ParameterStatus,
    b"K": BackendKeyData,
    b"Z": ReadyForQuery,
    b"T": RowDescription,
    b"E": ErrorResponse,
    b"D": DataRow,
    b"C": CommandComplete,
    b"1": ParseComplete,
    b"2": BindComplete,
    b"3": CloseComplete,
    b"s": PortalSuspended,
    b"n": NoData,
    b"t": ParameterDescription,
    b"A": NotificationResponse,
    b"c": CopyDone,
    b"d": CopyData,
    b"G": CopyInResponse,
    b"H": CopyOutResponse,
}
