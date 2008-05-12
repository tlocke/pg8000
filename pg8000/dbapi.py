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

import datetime
import time
import interface
import types
from errors import *

import logging
logging = logging.getLogger("pg8000")

apilevel = "2.0"
threadsafety = 3
paramstyle = 'format' # paramstyle can be changed to any DB-API paramstyle

def convert_paramstyle(src_style, query, args):
    logging.debug("convert_paramstyle, %r, %r, %r", src_style, query, args)
    # I don't see any way to avoid scanning the query string char by char,
    # so we might as well take that careful approach and create a
    # state-based scanner.  We'll use int variables for the state.
    #  0 -- outside quoted string
    #  1 -- inside single-quote string '...'
    #  2 -- inside quoted identifier   "..."
    #  3 -- inside escaped single-quote string, E'...'
    state = 0
    output_query = ""
    output_args = []
    if src_style == "numeric":
        output_args = args
    elif src_style in ("pyformat", "named"):
        mapping_to_idx = {}
    i = 0
    while 1:
        if i == len(query):
            break
        c = query[i]
        # print "begin loop", repr(i), repr(c), repr(state)
        if state == 0:
            if c == "'":
                i += 1
                output_query += c
                state = 1
            elif c == '"':
                i += 1
                output_query += c
                state = 2
            elif c == 'E':
                # check for escaped single-quote string
                i += 1
                if i < len(query) and i > 1 and query[i] == "'":
                    i += 1
                    output_query += "E'"
                    state = 3
                else:
                    output_query += c
            elif src_style == "qmark" and c == "?":
                i += 1
                param_idx = len(output_args)
                if param_idx == len(args):
                    raise ProgrammingError("too many parameter fields, not enough parameters")
                output_args.append(args[param_idx])
                output_query += "$" + str(param_idx + 1)
            elif src_style == "numeric" and c == ":":
                i += 1
                if i < len(query) and i > 1 and query[i].isdigit():
                    output_query += "$" + query[i]
                    i += 1
                else:
                    raise ProgrammingError("numeric parameter : does not have numeric arg")
            elif src_style == "named" and c == ":":
                name = ""
                while 1:
                    i += 1
                    if i == len(query):
                        break
                    c = query[i]
                    if c.isalnum():
                        name += c
                    else:
                        break
                if name == "":
                    raise ProgrammingError("empty name of named parameter")
                idx = mapping_to_idx.get(name)
                if idx == None:
                    idx = len(output_args)
                    output_args.append(args[name])
                    idx += 1
                    mapping_to_idx[name] = idx
                output_query += "$" + str(idx)
            elif src_style == "format" and c == "%":
                i += 1
                if i < len(query) and i > 1:
                    if query[i] == "s":
                        param_idx = len(output_args)
                        if param_idx == len(args):
                            raise ProgrammingError("too many parameter fields, not enough parameters")
                        output_args.append(args[param_idx])
                        output_query += "$" + str(param_idx + 1)
                    elif query[i] == "%":
                        output_query += "%"
                    else:
                        raise ProgrammingError("Only %s and %% are supported")
                    i += 1
                else:
                    raise ProgrammingError("numeric parameter : does not have numeric arg")
            elif src_style == "pyformat" and c == "%":
                i += 1
                if i < len(query) and i > 1:
                    if query[i] == "(":
                        i += 1
                        # begin mapping name
                        end_idx = query.find(')', i)
                        if end_idx == -1:
                            raise ProgrammingError("began pyformat dict read, but couldn't find end of name")
                        else:
                            name = query[i:end_idx]
                            i = end_idx + 1
                            if i < len(query) and query[i] == "s":
                                i += 1
                                idx = mapping_to_idx.get(name)
                                if idx == None:
                                    idx = len(output_args)
                                    output_args.append(args[name])
                                    idx += 1
                                    mapping_to_idx[name] = idx
                                output_query += "$" + str(idx)
                            else:
                                raise ProgrammingError("format not specified or not supported (only %(...)s supported)")
                    elif query[i] == "%":
                        output_query += "%"
            else:
                i += 1
                output_query += c
        elif state == 1:
            output_query += c
            i += 1
            if c == "'":
                # Could be a double ''
                if i < len(query) and query[i] == "'":
                    # is a double quote.
                    output_query += query[i]
                    i += 1
                else:
                    state = 0
            elif src_style in ("pyformat","format") and c == "%":
                # hm... we're only going to support an escaped percent sign
                if i < len(query):
                    if query[i] == "%":
                        # good.  We already output the first percent sign.
                        i += 1
                    else:
                        raise ProgrammingError("'%" + query[i] + "' not supported in quoted string")
        elif state == 2:
            output_query += c
            i += 1
            if c == '"':
                state = 0
            elif src_style in ("pyformat","format") and c == "%":
                # hm... we're only going to support an escaped percent sign
                if i < len(query):
                    if query[i] == "%":
                        # good.  We already output the first percent sign.
                        i += 1
                    else:
                        raise ProgrammingError("'%" + query[i] + "' not supported in quoted string")
        elif state == 3:
            output_query += c
            i += 1
            if c == "\\":
                # check for escaped single-quote
                if i < len(query) and query[i] == "'":
                    output_query += "'"
                    i += 1
            elif c == "'":
                state = 0
            elif src_style in ("pyformat","format") and c == "%":
                # hm... we're only going to support an escaped percent sign
                if i < len(query):
                    if query[i] == "%":
                        # good.  We already output the first percent sign.
                        i += 1
                    else:
                        raise ProgrammingError("'%" + query[i] + "' not supported in quoted string")

    return output_query, tuple(output_args)


class CursorWrapper(object):
    def __init__(self, conn):
        self.cursor = interface.Cursor(conn)
        self.arraysize = 1
        self._override_rowcount = None

    rowcount = property(lambda self: self._getRowCount())
    def _getRowCount(self):
        if self.cursor == None:
            raise InterfaceError("cursor is closed")
        if self._override_rowcount != None:
            return self._override_rowcount
        return self.cursor.row_count

    description = property(lambda self: self._getDescription())
    def _getDescription(self):
        if self.cursor.row_description == None:
            return None
        columns = []
        for col in self.cursor.row_description:
            columns.append((col["name"], col["type_oid"], None, None, None, None, None))
        return columns

    def execute(self, operation, args=()):
        logging.debug("CursorWrapper.execute, %r, %r", operation, args)
        if self.cursor == None:
            raise InterfaceError("cursor is closed")
        self._override_rowcount = None
        self._execute(operation, args)

    def _execute(self, operation, args=()):
        new_query, new_args = convert_paramstyle(paramstyle, operation, args)
        try:
            self.cursor.execute(new_query, *new_args)
        except:
            # any error will rollback the transaction to-date
            self.cursor.connection.rollback()
            raise

    def executemany(self, operation, parameter_sets):
        self._override_rowcount = 0
        for parameters in parameter_sets:
            self._execute(operation, parameters)
            if self.cursor.row_count == -1 or self._override_rowcount == -1:
                self._override_rowcount = -1
            else:
                self._override_rowcount += self.cursor.row_count

    def fetchone(self):
        logging.debug("CursorWrapper.fetchone")
        if self.cursor == None:
            raise InterfaceError("cursor is closed")
        return self.cursor.read_tuple()

    def fetchmany(self, size=None):
        logging.debug("CursorWrapper.fetchmany")
        if size == None:
            size = self.arraysize
        rows = []
        for i in range(size):
            value = self.fetchone()
            if value == None:
                break
            rows.append(value)
        return rows

    def fetchall(self):
        logging.debug("CursorWrapper.fetchall")
        if self.cursor == None:
            raise InterfaceError("cursor is closed")
        return tuple(self.cursor.iterate_tuple())

    def close(self):
        logging.debug("CursorWrapper.close")
        self.cursor = None
        self._override_rowcount = None

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

class ConnectionWrapper(object):
    # DBAPI Extension: supply exceptions as attributes on the connection
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, **kwargs):
        self.conn = interface.Connection(**kwargs)
        self.conn.begin()

    def cursor(self):
        return CursorWrapper(self.conn)

    def commit(self):
        logging.debug("ConnectionWrapper.commit")
        # There's a threading bug here.  If a query is sent after the
        # commit, but before the begin, it will be executed immediately
        # without a surrounding transaction.  Like all threading bugs -- it
        # sounds unlikely, until it happens every time in one
        # application...  however, to fix this, we need to lock the
        # database connection entirely, so that no cursors can execute
        # statements on other threads.  Support for that type of lock will
        # be done later.
        if self.conn == None:
            raise InterfaceError("connection is closed")
        self.conn.commit()
        self.conn.begin()

    def rollback(self):
        logging.debug("ConnectionWrapper.rollback")
        # see bug description in commit.
        if self.conn == None:
            raise InterfaceError("connection is closed")
        self.conn.rollback()
        self.conn.begin()

    def close(self):
        logging.debug("ConnectionWrapper.close")
        if self.conn == None:
            raise InterfaceError("connection is closed")
        self.conn.close()
        self.conn = None

def connect(user, host=None, unix_sock=None, port=5432, database=None, password=None, socket_timeout=60, ssl=False):
    return ConnectionWrapper(user=user, host=host,
            unix_sock=unix_sock, port=port, database=database,
            password=password, socket_timeout=socket_timeout, ssl=ssl)

def Date(year, month, day):
    return datetime.date(year, month, day)

def Time(hour, minute, second):
    return datetime.time(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

def Binary(value):
    return types.Bytea(value)

# I have no idea what this would be used for by a client app.  Should it be
# TEXT, VARCHAR, CHAR?  It will only compare against row_description's
# type_code if it is this one type.  It is the varchar type oid for now, this
# appears to match expectations in the DB API 2.0 compliance test suite.
STRING = 1043

# bytea type_oid
BINARY = 17

# numeric type_oid
NUMBER = 1700

# timestamp type_oid
DATETIME = 1114

# oid type_oid
ROWID = 26


