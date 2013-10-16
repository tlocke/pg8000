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
from __future__ import absolute_import

__author__ = "Mathieu Fenniak"

import socket
try:
    import ssl as sslmodule
except ImportError:
    sslmodule = None
import select
import threading
import struct
import hashlib
import collections

from . import types, errors, util


class SendMessage(object):
    pass


class ReceiveMessage(object):
    pass


##
# An SSLRequest message.  To initiate an SSL-encrypted connection, an
# SSLRequest message is used rather than a {@link StartupMessage
# StartupMessage}.  A StartupMessage is still sent, but only after SSL
# negotiation (if accepted).
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class SSLRequest(SendMessage):
    # Int32(8) - Message length, including self.<br>
    # Int32(80877103) - The SSL request code.<br>
    def serialize(self):
        return struct.pack("!ii", 8, 80877103)


##
# A StartupMessage message.  Begins a DB session, identifying the user to be
# authenticated as and the database to connect to.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class StartupMessage(SendMessage):
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
        val = struct.pack("!i", protocol)
        val += "user\x00" + self.user + "\x00"
        if self.database:
            val += "database\x00" + self.database + "\x00"
        val += "\x00"
        val = struct.pack("!i", len(val) + 4) + val
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
class Parse(SendMessage):
    def __init__(self, ps, qs, type_oids):
        if isinstance(qs, unicode):
            raise TypeError("qs must be encoded byte data")
        self.ps = ps
        self.qs = qs
        self.type_oids = type_oids

    # Byte1('P') - Identifies the message as a Parse command.
    # Int32 -   Message length, including self.
    # String -  Prepared statement name.  An empty string selects the unnamed
    #           prepared statement.
    # String -  The query string.
    # Int16 -   Number of parameter data types specified (can be zero).
    # For each parameter:
    #   Int32 - The OID of the parameter data type.
    def serialize(self):
        val = self.ps + "\x00" + self.qs + "\x00"
        val = val + struct.pack("!h", len(self.type_oids))
        for oid in self.type_oids:
            # Parse message doesn't seem to handle the -1 type_oid for NULL
            # values that other messages handle. So we'll provide type_oid 705,
            # the PG "unknown" type.
            if oid == -1:
                oid = 705
            val = val + struct.pack("!i", oid)
        val = struct.pack("!i", len(val) + 4) + val
        val = "P" + val
        return val

    def __repr__(self):
        return "<Parse ps=%r qs=%r>" % (self.ps, self.qs)


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
class Bind(SendMessage):
    def __init__(self, portal, ps, in_fc, params, out_fc, **kwargs):
        self.portal = portal
        self.ps = ps
        self.in_fc = in_fc
        self.params = []

        if not self.in_fc:
            fc = 0
        elif len(self.in_fc) == 1:
            fc = self.in_fc[0]
        else:
            fc = None

        self.params = [
            types.pg_value(
                param, self.in_fc[i] if fc is None else fc, **kwargs)
            for i, param in enumerate(params)
        ]
        self.out_fc = out_fc

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
        retval = ""
        retval += self.portal + "\x00"
        retval += self.ps + "\x00"
        retval += struct.pack("!h", len(self.in_fc))
        for fc in self.in_fc:
            retval += struct.pack("!h", fc)
        retval += struct.pack("!h", len(self.params))
        for param in self.params:
            if param is None:
                # special case, NULL value
                retval += struct.pack("!i", -1)
            else:
                retval += struct.pack("!i", len(param))
                retval += param
        retval += struct.pack("!h", len(self.out_fc))
        for fc in self.out_fc:
            retval += struct.pack("!h", fc)
        retval = struct.pack("!i", len(retval) + 4) + retval
        retval = "B" + retval
        return retval

    def __repr__(self):
        return "<Bind p=%r s=%r>" % (self.portal, self.ps)


##
# A Close message, used for closing prepared statements and portals.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param typ    'S' for prepared statement, 'P' for portal.
# @param name   The name of the item to close.
class Close(SendMessage):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise errors.InternalError("Close typ must be 1 char")
        self.typ = typ
        self.name = name

    # Byte1('C') - Identifies the message as a close command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def serialize(self):
        val = self.typ + self.name + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "C" + val
        return val


##
# A specialized Close message for a portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ClosePortal(Close):
    def __init__(self, name):
        Close.__init__(self, "P", name)


##
# A specialized Close message for a prepared statement.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ClosePreparedStatement(Close):
    def __init__(self, name):
        Close.__init__(self, "S", name)


##
# A Describe message, used for obtaining information on prepared statements
# and portals.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
#
# @param typ    'S' for prepared statement, 'P' for portal.
# @param name   The name of the item to close.
class Describe(SendMessage):
    def __init__(self, typ, name):
        if len(typ) != 1:
            raise errors.InternalError("Describe typ must be 1 char")
        self.typ = typ
        self.name = name

    # Byte1('D') - Identifies the message as a describe command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def serialize(self):
        val = self.typ + self.name + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "D" + val
        return val


##
# A specialized Describe message for a portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class DescribePortal(Describe):
    def __init__(self, name):
        Describe.__init__(self, "P", name)

    def __repr__(self):
        return "<DescribePortal %r>" % (self.name)


##
# A specialized Describe message for a prepared statement.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class DescribePreparedStatement(Describe):
    def __init__(self, name):
        Describe.__init__(self, "S", name)

    def __repr__(self):
        return "<DescribePreparedStatement %r>" % (self.name)


##
# A Flush message forces the backend to deliver any data pending in its
# output buffers.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Flush(SendMessage):
    # Byte1('H') - Identifies the message as a flush command.
    # Int32(4) - Length of message, including self.
    def serialize(self):
        return 'H\x00\x00\x00\x04'

    def __repr__(self):
        return "<Flush>"
_FLUSH = Flush().serialize()


##
# Causes the backend to close the current transaction (if not in a BEGIN/COMMIT
# block), and issue ReadyForQuery.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Sync(SendMessage):
    # Byte1('S') - Identifies the message as a sync command.
    # Int32(4) - Length of message, including self.
    def serialize(self):
        return 'S\x00\x00\x00\x04'

    def __repr__(self):
        return "<Sync>"
_SYNC = Sync().serialize()


##
# Transmits a password.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class PasswordMessage(SendMessage):
    def __init__(self, pwd):
        self.pwd = pwd

    # Byte1('p') - Identifies the message as a password message.
    # Int32 - Message length including self.
    # String - The password.  Password may be encrypted.
    def serialize(self):
        val = self.pwd + "\x00"
        val = struct.pack("!i", len(val) + 4) + val
        val = "p" + val
        return val


##
# Requests that the backend execute a portal and retrieve any number of rows.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
# @param row_count  The number of rows to return.  Can be zero to indicate the
#                   backend should return all rows. If the portal represents a
#                   query that does not return rows, no rows will be returned
#                   no matter what the row_count.
class Execute(SendMessage):
    def __init__(self, portal, row_count):
        self.portal = portal
        self.row_count = row_count

    # Byte1('E') - Identifies the message as an execute message.
    # Int32 -   Message length, including self.
    # String -  The name of the portal to execute.
    # Int32 -   Maximum number of rows to return, if portal contains a query
    #           that returns rows.  0 = no limit.
    def serialize(self):
        val = self.portal + "\x00" + struct.pack("!i", self.row_count)
        val = struct.pack("!i", len(val) + 4) + val
        val = "E" + val
        return val


##
# Informs the backend that the connection is being closed.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class Terminate(SendMessage):
    # Byte1('X') - Identifies the message as a terminate message.
    # Int32(4) - Message length, including self.
    def serialize(self):
        return 'X\x00\x00\x00\x04'


##
# Base class of all Authentication[*] messages.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class AuthenticationRequest(SendMessage):
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
    def create_from_data(data):
        ident = struct.unpack("!i", data[:4])[0]
        klass = authentication_codes.get(ident, None)
        if klass:
            return klass(data[4:])
        else:
            raise errors.NotSupportedError(
                "authentication method %r not supported" % (ident,))

    def ok(self, conn, user, **kwargs):
        raise errors.InternalError(
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
# ParameterStatus message sent from backend, used to inform the frotnend of
# runtime configuration parameter changes.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ParameterStatus(ReceiveMessage):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    # Byte1('S') - Identifies ParameterStatus
    # Int32 - Message length, including self.
    # String - Runtime parameter name.
    # String - Runtime parameter value.
    @staticmethod
    def create_from_data(data):
        key = data[:data.find("\x00")]
        value = data[data.find("\x00") + 1:-1]
        return ParameterStatus(key, value)


##
# BackendKeyData message sent from backend.  Contains a connection's process
# ID and a secret key.  Can be used to terminate the connection's current
# actions, such as a long running query.  Not supported by pg8000 yet.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class BackendKeyData(ReceiveMessage):
    def __init__(self, process_id, secret_key):
        self.process_id = process_id
        self.secret_key = secret_key

    # Byte1('K') - Identifier.
    # Int32(12) - Message length, including self.
    # Int32 - Process ID.
    # Int32 - Secret key.
    @staticmethod
    def create_from_data(data):
        process_id, secret_key = struct.unpack("!2i", data)
        return BackendKeyData(process_id, secret_key)


##
# Message representing a query with no data.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class NoData(ReceiveMessage):
    # Byte1('n') - Identifier.
    # Int32(4) - Message length, including self.
    @staticmethod
    def create_from_data(data):
        return _NO_DATA
_NO_DATA = NoData()


##
# Message representing a successful Parse.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ParseComplete(ReceiveMessage):
    # Byte1('1') - Identifier.
    # Int32(4) - Message length, including self.
    @staticmethod
    def create_from_data(data):
        return _PARSE_COMPLETE
_PARSE_COMPLETE = ParseComplete()


##
# Message representing a successful Bind.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class BindComplete(ReceiveMessage):
    # Byte1('2') - Identifier.
    # Int32(4) - Message length, including self.
    @staticmethod
    def create_from_data(data):
        return _BIND_COMPLETE
_BIND_COMPLETE = BindComplete()


##
# Message representing a successful Close.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class CloseComplete(ReceiveMessage):
    # Byte1('3') - Identifier.
    # Int32(4) - Message length, including self.
    @staticmethod
    def create_from_data(data):
        return _CLOSE_COMPLETE
_CLOSE_COMPLETE = CloseComplete()


##
# Message representing data from an Execute has been received, but more data
# exists in the portal.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class PortalSuspended(ReceiveMessage):
    # Byte1('s') - Identifier.
    # Int32(4) - Message length, including self.
    @staticmethod
    def create_from_data(data):
        return _PORTAL_SUSPENDED
_PORTAL_SUSPENDED = PortalSuspended()


##
# Message representing the backend is ready to process a new query.
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
class ReadyForQuery(ReceiveMessage):
    def __init__(self, status):
        # I = Idle, T = Idle in Transaction, E = idle in failed transaction.
        self.status = status

    def __repr__(self):
        return "<ReadyForQuery %s>" % {
            "I": "Idle", "T": "Idle in Transaction",
            "E": "Idle in Failed Transaction"}[self.status]

    # Byte1('Z') - Identifier.
    # Int32(5) - Message length, including self.
    # Byte1 -   Status indicator.
    @staticmethod
    def create_from_data(data):
        return ReadyForQuery(data)


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
class NoticeResponse(ReceiveMessage):
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
        return "<NoticeResponse %s %s %r>" % (
            self.severity, self.code, self.msg)

    @staticmethod
    def dataIntoDict(data):
        retval = {}
        for s in data.split("\x00"):
            if not s:
                continue
            key, value = s[0], s[1:]
            key = NoticeResponse.responseKeys.get(key, key)
            retval[key] = value
        return retval

    # Byte1('N') - Identifier
    # Int32 - Message length
    # Any number of these, followed by a zero byte:
    #   Byte1 - code identifying the field type (see responseKeys)
    #   String - field value
    @staticmethod
    def create_from_data(data):
        return NoticeResponse(**NoticeResponse.dataIntoDict(data))


##
# A message sent in case of a server-side error.  Contains the same properties
# that {@link NoticeResponse NoticeResponse} contains.
# <p>
# Stability: Added in pg8000 v1.03.  Required properties severity, code, and
# msg are guaranteed for v1.xx.  Other properties should be checked with
# hasattr before accessing.
class ErrorResponse(ReceiveMessage):
    def __init__(self, **kwargs):
        for arg, value in kwargs.items():
            setattr(self, arg, value)

    def __repr__(self):
        return "<ErrorResponse %s %s %r>" % (
            self.severity, self.code, self.msg)

    def createException(self):
        return errors.ProgrammingError(self.severity, self.code, self.msg)

    @staticmethod
    def create_from_data(data):
        return ErrorResponse(**NoticeResponse.dataIntoDict(data))


##
# A message sent if this connection receives a NOTIFY that it was LISTENing
# for.
# <p>
# Stability: Added in pg8000 v1.03.  When limited to accessing properties from
# a notification event dispatch, stability is guaranteed for v1.xx.
class NotificationResponse(ReceiveMessage):
    def __init__(self, backend_pid, condition, additional_info):
        self.backend_pid = backend_pid
        self.condition = condition
        self.additional_info = additional_info

    ##
    # An integer representing the process ID of the backend that triggered
    # the NOTIFY.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    backend_pid = None

    ##
    # The name of the notification fired.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    condition = None

    ##
    # Currently unspecified by the PostgreSQL documentation as of v8.3.1.
    # <p>
    # Stability: Added in pg8000 v1.03, stability guaranteed for v1.xx.
    additional_info = None

    def __repr__(self):
        return "<NotificationResponse %s %s %r>" % (
            self.backend_pid, self.condition, self.additional_info)

    @staticmethod
    def create_from_data(data):
        backend_pid = struct.unpack("!i", data[:4])[0]
        data = data[4:]
        null = data.find("\x00")
        condition = data[:null]
        data = data[null + 1:]
        null = data.find("\x00")
        additional_info = data[:null]
        return NotificationResponse(backend_pid, condition, additional_info)


class ParameterDescription(ReceiveMessage):
    def __init__(self, type_oids):
        self.type_oids = type_oids

    @staticmethod
    def create_from_data(data):
        count = struct.unpack("!h", data[:2])[0]
        type_oids = struct.unpack("!" + "i" * count, data[2:])
        return ParameterDescription(type_oids)


class RowDescription(ReceiveMessage):
    def __init__(self, fields):
        self.fields = fields

    @staticmethod
    def create_from_data(data):
        count = struct.unpack("!h", data[:2])[0]
        data = data[2:]
        fields = []
        for i in range(count):
            null = data.find("\x00")
            field = {"name": data[:null]}
            data = data[null + 1:]
            field["table_oid"], field["column_attrnum"], field["type_oid"], \
                field["type_size"], field["type_modifier"], \
                field["format"] = struct.unpack("!ihihih", data[:18])
            data = data[18:]
            fields.append(field)
        return RowDescription(fields)


class CommandComplete(ReceiveMessage):
    def __init__(self, command, rows=None, oid=None):
        self.command = command
        self.rows = rows
        self.oid = oid

    @staticmethod
    def create_from_data(data):
        values = data[:-1].split(" ")
        args = {}
        args['command'] = values[0]
        if args['command'] in (
                "INSERT", "DELETE", "UPDATE", "MOVE", "FETCH", "COPY"):
            args['rows'] = int(values[-1])
            if args['command'] == "INSERT":
                args['oid'] = int(values[1])
        else:
            args['command'] = data[:-1]
        return CommandComplete(**args)


class DataRow(ReceiveMessage):
    def __init__(self, fields):
        self.fields = fields

    @staticmethod
    def create_from_data(data):
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


class CopyData(ReceiveMessage):
    # "d": CopyData,
    def __init__(self, data):
        self.data = data

    @staticmethod
    def create_from_data(data):
        return CopyData(data)

    def serialize(self):
        return 'd' + struct.pack('!i', len(self.data) + 4) + self.data


class CopyDone(SendMessage, ReceiveMessage):
    # Byte1('c') - Identifier.
    # Int32(4) - Message length, including self.

    @staticmethod
    def create_from_data(data):
        return _COPY_DONE

    def serialize(self):
        return 'c\x00\x00\x00\x04'

_COPY_DONE = CopyDone()
_COPY_DONE_SERIALIZED = _COPY_DONE.serialize()


class CopyOutResponse(ReceiveMessage):
    # Byte1('H')
    # Int32(4) - Length of message contents in bytes, including self.
    # Int8(1) - 0 textual, 1 binary
    # Int16(2) - Number of columns
    # Int16(N) - Format codes for each column (0 text, 1 binary)

    def __init__(self, is_binary, column_formats):
        self.is_binary = is_binary
        self.column_formats = column_formats

    @staticmethod
    def create_from_data(data):
        is_binary, num_cols = struct.unpack('!bh', data[:3])
        column_formats = struct.unpack('!' + ('h' * num_cols), data[3:])
        return CopyOutResponse(is_binary, column_formats)


class CopyInResponse(ReceiveMessage):
    # Byte1('G')
    # Otherwise the same as CopyOutResponse

    def __init__(self, is_binary, column_formats):
        self.is_binary = is_binary
        self.column_formats = column_formats

    @staticmethod
    def create_from_data(data):
        is_binary, num_cols = struct.unpack('!bh', data[:3])
        column_formats = struct.unpack('!' + ('h' * num_cols), data[3:])
        return CopyInResponse(is_binary, column_formats)


SUCCESS_READ_LOOP = util.symbol('success_read_loop')
CONTINUE_READ_LOOP = util.symbol('CONTINUE_READ_LOOP')

STATE_READY = util.symbol('state_ready')
STATE_CLOSED = util.symbol('state_closed')
STATE_NOAUTH = util.symbol('state_noauth')
STATE_AUTH = util.symbol('state_auth')


class MessageReader(object):
    def __init__(self, args):
        self.args = collections.namedtuple("reader_args", args)
        self._msgs = {}

        self.add_message(NoticeResponse, self._notice_response)
        self.add_message(ParameterStatus, self._parameter_status)
        self.add_message(NotificationResponse, self._notification_response)

        # If true, raise exception from an ErrorResponse after messages are
        # processed.  This can be used to leave the connection in a usable
        # state after an error response, rather than having unconsumed
        # messages that won't be understood in another context.
        self.delay_raising_exception = False

        self.ignore_unhandled_messages = False

    def _notice_response(self, conn, msg, ctx):
        conn.handleNoticeResponse(msg)
        return CONTINUE_READ_LOOP

    def _parameter_status(self, conn, msg, ctx):
        conn.handleParameterStatus(msg)
        return CONTINUE_READ_LOOP

    def _notification_response(self, conn, msg, ctx):
        conn.handleNotificationResponse(msg)
        return CONTINUE_READ_LOOP

    def add_message(self, msg_class, handler):
        # store the handler keyed against
        # the given message class as well as all
        # subclasses, so that any message subclass can be
        # matched with one lookup
        stack = [msg_class]
        while stack:
            cls_ = stack.pop()
            stack.extend(cls_.__subclasses__())
            self._msgs[cls_] = handler

    def __call__(self, conn, *args):
        args = self.args(*args)
        exc = None
        while True:
            msg = conn._read_message()

            cls_key = msg.__class__
            try:
                handler = self._msgs[cls_key]
            except KeyError:
                if isinstance(msg, ErrorResponse):
                    exc = msg.createException()
                    if not self.delay_raising_exception:
                        raise exc
                elif not self.ignore_unhandled_messages:
                    raise errors.InternalError(
                        "Unexpected response msg %r" % (msg))
            else:
                if handler is SUCCESS_READ_LOOP:
                    if exc:
                        raise exc
                    return True
                elif handler is CONTINUE_READ_LOOP:
                    continue

                retval = handler(conn, msg, args)
                if retval is SUCCESS_READ_LOOP:
                    if exc:
                        raise exc
                    return True
                elif retval is CONTINUE_READ_LOOP:
                    continue
                else:
                    return retval


##
# A message representing the backend requesting an MD5 hashed password
# response.  The response will be sent as md5(md5(pwd + login) + salt).
# <p>
# Stability: This is an internal class.  No stability guarantee is made.
# TODO: this isn't covered !  not tested
class AuthenticationMD5Password(AuthenticationRequest):
    # Additional message data:
    #  Byte4 - Hash salt.
    def __init__(self, data):
        self.salt = "".join(struct.unpack("4c", data))

    def ok(self, conn, user, password=None, **kwargs):
        if not password:
            raise errors.InterfaceError(
                "server requesting MD5 password authentication, "
                "but no password was provided")
        pwd = "md5" + hashlib.md5(
            hashlib.md5(password + user).hexdigest() + self.salt).hexdigest()
        conn._flush_messages(
            PasswordMessage(
                pwd).serialize())

        return self._authentication_reader(conn, self, user)

    @util.class_memoized
    def _authentication_reader():
        reader = MessageReader(args=('pw', 'user',))
        reader.add_message(
            AuthenticationRequest,
            lambda conn, msg, context: msg.ok(conn, context.user))
        reader.add_message(
            ErrorResponse,
            lambda conn, msg, context: context.pw._ok_error(msg))
        return reader

    def _ok_error(self, msg):
        if msg.code == "28000":
            raise errors.InterfaceError("md5 password authentication failed")
        else:
            raise msg.createException()

authentication_codes = {
    0: AuthenticationOk,
    5: AuthenticationMD5Password,
}


def sync_on_error(fn):
    def _fn(self, *args, **kwargs):
        try:
            self._sock_lock.acquire()
            return fn(self, *args, **kwargs)
        except:
            self._sync()
            raise
        finally:
            self._sock_lock.release()
    return _fn


class Connection(object):
    def __init__(
            self, unix_sock=None, host=None, port=5432, socket_timeout=60,
            ssl=False):
        self._client_encoding = "ascii"
        self._integer_datetimes = False
        self._server_version = None
        self._sock_buf = ""
        self._sock_lock = threading.Lock()
        if unix_sock is None and host is not None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif unix_sock is not None:
            if not hasattr(socket, "AF_UNIX"):
                raise errors.InterfaceError(
                    "attempt to connect to unix socket on "
                    "unsupported platform")
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            raise errors.ProgrammingError(
                "one of host or unix_sock must be provided")
        if unix_sock is None and host is not None:
            self._sock.connect((host, port))
        elif unix_sock is not None:
            self._sock.connect(unix_sock)
        if ssl:
            self._sock_lock.acquire()
            try:
                self._flush_messages(SSLRequest().serialize())
                resp = self._sock.recv(1)
                if resp == 'S' and sslmodule is not None:
                    self._sock = sslmodule.wrap_socket(self._sock)
                elif sslmodule is None:
                    raise errors.InterfaceError(
                        "SSL required but ssl module not available in "
                        "this python installation")
                else:
                    raise errors.InterfaceError("server refuses SSL")
            finally:
                self._sock_lock.release()
        else:
            # settimeout causes ssl failure, on windows.  Python bug 1462352.
            self._sock.settimeout(socket_timeout)
        self._state = STATE_NOAUTH
        self._backend_key_data = None

        self.NoticeReceived = util.MulticastDelegate()
        self.ParameterStatusReceived = util.MulticastDelegate()
        self.NotificationReceived = util.MulticastDelegate()

        self.ParameterStatusReceived += self._onParameterStatusReceived

    def _invalid_state(self, state):
        raise errors.InternalError(
            "connection state must be %s, is %s" % (state, self._state))

    def _flush_messages(self, *msg):
        self._sock.sendall("".join(msg))

    def _read_bytes(self, byte_count):
        retval = ""
        bytes_read = 0
        while bytes_read < byte_count:
            if not self._sock_buf:
                self._sock_buf = self._sock.recv(1024)
            data = self._sock_buf[0:byte_count - bytes_read]
            self._sock_buf = self._sock_buf[byte_count - bytes_read:]
            bytes_read += len(data)
            retval += data
        return retval

    def _read_message(self):
        #assert self._sock_lock.locked()
        bytes = self._read_bytes(5)
        message_code = bytes[0]
        data_len = struct.unpack("!i", bytes[1:])[0] - 4
        bytes = self._read_bytes(data_len)
        #assert len(bytes) == data_len
        msg = message_types[message_code].create_from_data(bytes)
        #print "_read_message() -> %r" % msg
        return msg

    def authenticate(self, user, **kwargs):
        if self._state is not STATE_NOAUTH:
            self._invalid_state(STATE_NOAUTH)
        self._sock_lock.acquire()
        try:
            self._flush_messages(
                StartupMessage(
                    user, database=kwargs.get("database", None)).
                serialize())

            self._authenticate_reader(self, user, kwargs)
        finally:
            self._sock_lock.release()

    @util.class_memoized
    def _authenticate_reader():
        reader = MessageReader(args=('user', 'kwargs'))

        def auth_request(connection, msg, context):
            return connection._authentication_request(
                msg, context.user, **context.kwargs)
        reader.add_message(AuthenticationRequest, auth_request)
        return reader

    def _authentication_request(self, msg, user, **kwargs):
        #assert self._sock_lock.locked()
        if not msg.ok(self, user, **kwargs):
            raise errors.InterfaceError(
                "authentication method %s failed" % msg.__class__.__name__)
        self._state = STATE_AUTH
        self._auth_request_reader(self)
        return SUCCESS_READ_LOOP

    @util.class_memoized
    def _auth_request_reader():
        reader = MessageReader(args=())
        reader.add_message(
            ReadyForQuery,
            lambda conn, msg, context: conn._ready_for_query(msg))
        reader.add_message(
            BackendKeyData,
            lambda conn, msg, context:
            conn._receive_backend_key_data(msg))
        return reader

    def _ready_for_query(self, msg):
        self._state = STATE_READY
        return SUCCESS_READ_LOOP

    def _receive_backend_key_data(self, msg):
        self._backend_key_data = msg
        return CONTINUE_READ_LOOP

    @sync_on_error
    def parse(self, statement, qs, param_types):
        if self._state is not STATE_READY:
            self._invalid_state(STATE_READY)

        type_info = [types.pg_type_info(x) for x in param_types]
        param_types, param_fc = [x[0] for x in type_info], \
                                [x[1] for x in type_info]
        self._flush_messages(
            Parse(
                statement, qs.encode(self._client_encoding),
                param_types).serialize(),
            DescribePreparedStatement(statement).serialize(),
            _FLUSH)

        return self._parse_reader(self, param_fc)

    @util.class_memoized
    def _parse_reader():
        reader = MessageReader(args=('param_fc',))
        # ParseComplete is good.
        reader.add_message(ParseComplete, CONTINUE_READ_LOOP)

        # Well, we don't really care -- we're going to send whatever we
        # want and let the database deal with it.  But thanks anyways!
        reader.add_message(ParameterDescription, CONTINUE_READ_LOOP)

        # We're not waiting for a row description.  Return something
        # destinctive to let bind know that there is no output.
        reader.add_message(
            NoData, lambda conn, msg, context: (None, context.param_fc))

        # Common row description response
        reader.add_message(
            RowDescription, lambda conn, msg, context: (msg, context.param_fc))
        return reader

    @sync_on_error
    def bind(self, portal, statement, params, parse_data, copy_stream):
        if self._state is not STATE_READY:
            self._invalid_state(STATE_READY)

        row_desc, param_fc = parse_data
        if row_desc is None:
            # no data coming out
            output_fc = ()
        else:
            # We've got row_desc that allows us to identify what we're going to
            # get back from this statement.
            output_fc = [types.py_type_info(f) for f in row_desc.fields]
        self._flush_messages(
            Bind(
                portal, statement, param_fc, params, output_fc,
                client_encoding=self._client_encoding,
                integer_datetimes=self._integer_datetimes).serialize(),

            # We need to describe the portal after bind, since the return
            # format codes will be different (hopefully, always what we
            # requested).
            DescribePortal(portal).serialize(),
            _FLUSH)

        # Read responses from server...
        return self._bind_reader(self, portal, copy_stream)

    @util.class_memoized
    def _bind_reader():
        reader = MessageReader(args=('portal', 'copy_stream'))

        # BindComplete is good -- just ignore
        reader.add_message(BindComplete, CONTINUE_READ_LOOP)

        # NoData in this case means we're not executing a query.  As a
        # result, we won't be fetching rows, so we'll never execute the
        # portal we just created... unless we execute it right away, which
        # we'll do.
        reader.add_message(
            NoData, lambda conn, msg, context: conn._bind_nodata(
                msg, context.portal, context.copy_stream))

        # Return the new row desc, since it will have the format types we
        # asked the server for
        reader.add_message(
            RowDescription, lambda conn, msg, context: (msg, None))
        return reader

    def _copy_in_response(self, copyin, fileobj):
        if fileobj is None:
            raise errors.CopyQueryWithoutStreamError()
        while True:
            data = fileobj.read(8192)
            if not data:
                break
            self._flush_messages(CopyData(data).serialize())
        self._flush_messages(
            _COPY_DONE_SERIALIZED,
            _SYNC
        )
        return CONTINUE_READ_LOOP

    def _copy_out_response(self, copyout, fileobj):
        if fileobj is None:
            raise errors.CopyQueryWithoutStreamError()

        self._copy_out_response_reader(self, fileobj)

        return CONTINUE_READ_LOOP

    @util.class_memoized
    def _copy_out_response_reader():
        reader = MessageReader(args=('fileobj',))
        reader.add_message(
            CopyData, lambda conn, msg, context:
            conn._copy_data(msg, context.fileobj))
        reader.add_message(CopyDone, SUCCESS_READ_LOOP)
        return reader

    def _copy_data(self, copydata, fileobj):
        fileobj.write(copydata.data)
        return CONTINUE_READ_LOOP

    def _bind_nodata(self, msg, portal, copy_stream):
        # Bind message returned NoData, causing us to execute the command.
        self._flush_messages(
            Execute(portal, 0).serialize(),
            _SYNC
        )

        output = {}
        reader = self._bind_nodata_response_reader
        reader(self, copy_stream, output)
        # TODO: would like to use a plain return value here, but not
        # sure if the output['msg'] thing allows for returning the
        # last of several "cmd_complete" messages, due to the
        # "setdefault()" line
        return (None, output['msg'])

    @util.class_memoized
    def _bind_nodata_response_reader():
        reader = MessageReader(args=('copy_stream', 'output'))
        reader.add_message(
            CopyOutResponse, lambda conn, msg, context:
            conn._copy_out_response(msg, context.copy_stream))
        reader.add_message(
            CopyInResponse, lambda conn, msg, context:
            conn._copy_in_response(msg, context.copy_stream))

        def cmd_complete(conn, msg, context):
            context.output.setdefault('msg', msg)
            return CONTINUE_READ_LOOP
        reader.add_message(CommandComplete, cmd_complete)
        reader.add_message(ReadyForQuery, SUCCESS_READ_LOOP)
        reader.delay_raising_exception = True
        return reader

    @sync_on_error
    def fetch_rows(self, portal, row_count, row_desc, adapter=None):
        if self._state is not STATE_READY:
            self._invalid_state(STATE_READY)

        self._flush_messages(
            Execute(portal, row_count).serialize(),
            _FLUSH
        )
        rows = []

        if adapter is None:
            adapter = self._default_datarow_reader
        retval = self._fetch_row_reader(self, rows, row_desc, portal, adapter)

        # retval = 2 when command complete, indicating that we've hit the
        # end of the available data for this command
        return (retval == 2), rows

    @util.class_memoized
    def _fetch_row_reader():
        reader = MessageReader(args=('rows', 'row_desc', 'portal', 'adapter'))

        def read_datarow(conn, msg, context):
            context.adapter(conn, msg, context.rows, context.row_desc)
            return CONTINUE_READ_LOOP
        reader.add_message(DataRow, read_datarow)
        reader.add_message(PortalSuspended, SUCCESS_READ_LOOP)
        reader.add_message(
            CommandComplete, lambda conn, msg, context:
            conn._fetch_commandcomplete(msg, context.portal))
        return reader

    @staticmethod
    def _default_datarow_reader(conn, msg, rows, row_desc):
        rows.append(
            [
                types.py_value(
                    msg.fields[i],
                    row_desc.fields[i],
                    client_encoding=conn._client_encoding,
                    integer_datetimes=conn._integer_datetimes,
                )
                for i in range(len(msg.fields))
            ]
        )

    def _fetch_commandcomplete(self, msg, portal):
        self._flush_messages(
            ClosePortal(portal).serialize(),
            _SYNC
        )

        self._fetch_commandcomplete_reader(self)

        return 2  # signal end-of-data

    @util.class_memoized
    def _fetch_commandcomplete_reader():
        reader = MessageReader(args=())
        reader.add_message(
            ReadyForQuery,
            lambda conn, msg, context: conn._fetch_commandcomplete_rfq(msg))
        reader.add_message(CloseComplete, CONTINUE_READ_LOOP)
        return reader

    def _fetch_commandcomplete_rfq(self, msg):
        self._state = STATE_READY
        return SUCCESS_READ_LOOP

    # Send a Sync message, then read and discard all messages until we
    # receive a ReadyForQuery message.
    def _sync(self):
        # it is assumed _sync is called from sync_on_error, which holds
        # a _sock_lock throughout the call
        self._flush_messages(_SYNC)
        self._sync_reader(self)

    @util.class_memoized
    def _sync_reader():
        reader = MessageReader(args=())
        reader.ignore_unhandled_messages = True
        reader.add_message(ReadyForQuery, SUCCESS_READ_LOOP)
        return reader

    def close_statement(self, statement):
        if self._state is STATE_CLOSED:
            return
        elif self._state is not STATE_READY:
            self._invalid_state(STATE_READY)
        self._sock_lock.acquire()
        try:
            self._flush_messages(
                ClosePreparedStatement(statement).serialize(),
                _SYNC
            )
            self._close_reader(self)
        finally:
            self._sock_lock.release()

    def close_portal(self, portal):
        if self._state is STATE_CLOSED:
            return
        elif self._state is not STATE_READY:
            self._invalid_state(STATE_READY)
        self._sock_lock.acquire()
        try:
            self._flush_messages(
                ClosePortal(portal).serialize(),
                _SYNC
            )

            self._close_reader(self)
        finally:
            self._sock_lock.release()

    @util.class_memoized
    def _close_reader():
        reader = MessageReader(args=())
        reader.add_message(CloseComplete, CONTINUE_READ_LOOP)
        reader.add_message(ReadyForQuery, SUCCESS_READ_LOOP)
        return reader

    def close(self):
        self._sock_lock.acquire()
        try:
            self._flush_messages(
                Terminate().serialize()
            )
            self._sock.close()
            self._state = STATE_CLOSED
        finally:
            self._sock_lock.release()

    def _onParameterStatusReceived(self, msg):
        if msg.key == "client_encoding":
            self._client_encoding = types.encoding_convert(msg.value)
        elif msg.key == "integer_datetimes":
            self._integer_datetimes = (msg.value == "on")
        elif msg.key == "server_version":
            self._server_version = msg.value

    def handleNoticeResponse(self, msg):
        self.NoticeReceived(msg)

    def handleParameterStatus(self, msg):
        self.ParameterStatusReceived(msg)

    def handleNotificationResponse(self, msg):
        self.NotificationReceived(msg)

    def fileno(self):
        # This should be safe to do without a lock
        return self._sock.fileno()

    def isready(self):
        self._sock_lock.acquire()
        try:
            rlst, _wlst, _xlst = select.select([self], [], [], 0)
            if not rlst:
                return False

            self._sync()
            return True
        finally:
            self._sock_lock.release()

    def server_version(self):
        if self._state is not STATE_READY:
            self._invalid_state(STATE_READY)
        if not self._server_version:
            raise errors.InterfaceError(
                "Server did not provide server_version parameter.")
        return self._server_version


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
    "A": NotificationResponse,
    "c": CopyDone,
    "d": CopyData,
    "G": CopyInResponse,
    "H": CopyOutResponse,
}
