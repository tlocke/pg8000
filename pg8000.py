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
import threading

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
    def __init__(self, obj, func):
        self.obj = obj
        self.func = func

    def __iter__(self):
        return self

    def next(self):
        retval = self.func(self.obj)
        if retval == None:
            raise StopIteration()
        return retval

class DBAPI(object):
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    InternalError = InternalError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    
    apilevel = "2.0"
    threadsafety = 3
    paramstyle = 'format' # paramstyle can be changed to any DB-API paramstyle

    def convert_paramstyle(src_style, query, args):
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
    convert_paramstyle = staticmethod(convert_paramstyle)


    class CursorWrapper(object):
        def __init__(self, conn):
            self.cursor = Cursor(conn)
            self.arraysize = 1

        rowcount = property(lambda self: self._getRowCount())
        def _getRowCount(self):
            return -1

        description = property(lambda self: self._getDescription())
        def _getDescription(self):
            if self.cursor.row_description == None:
                return None
            columns = []
            for col in self.cursor.row_description:
                columns.append((col["name"], col["type_oid"]))
            return columns

        def execute(self, operation, args=()):
            if self.cursor == None:
                raise InterfaceError("cursor is closed")
            new_query, new_args = DBAPI.convert_paramstyle(DBAPI.paramstyle, operation, args)
            self.cursor.execute(new_query, *new_args)

        def executemany(self, operation, parameter_sets):
            for parameters in parameter_sets:
                self.execute(operation, parameters)

        def fetchone(self):
            if self.cursor == None:
                raise InterfaceError("cursor is closed")
            return self.cursor.read_tuple()

        def fetchmany(self, size=None):
            if size == None:
                size = self.arraysize
            rows = []
            for i in range(size):
                rows.append(self.fetchone())
            return rows

        def fetchall(self):
            if self.cursor == None:
                raise InterfaceError("cursor is closed")
            return tuple(self.cursor.iterate_tuple())

        def close(self):
            self.cursor = None

        def setinputsizes(self, sizes):
            pass

        def setoutputsize(self, size, column=None):
            pass

    class ConnectionWrapper(object):
        def __init__(self, **kwargs):
            self.conn = Connection(**kwargs)
            self.conn.begin()

        def cursor(self):
            return DBAPI.CursorWrapper(self.conn)

        def commit(self):
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
            # see bug description in commit.
            if self.conn == None:
                raise InterfaceError("connection is closed")
            self.conn.rollback()
            self.conn.begin()

        def close(self):
            self.conn = None

    def connect(user, host=None, unix_sock=None, port=5432, database=None, password=None, socket_timeout=60):
        return DBAPI.ConnectionWrapper(user=user, host=host,
                unix_sock=unix_sock, port=port, database=database,
                password=password, socket_timeout=socket_timeout)
    connect = staticmethod(connect)


##
# This class represents a prepared statement.  A prepared statement is
# pre-parsed on the server, which reduces the need to parse the query every
# time it is run.  The statement can have parameters in the form of $1, $2, $3,
# etc.  When parameters are used, the types of the parameters need to be
# specified when creating the prepared statement.
# <p>
# As of v1.01, instances of this class are thread-safe.  This means that a
# single PreparedStatement can be accessed by multiple threads without the
# internal consistency of the statement being altered.  However, the
# responsibility is on the client application to ensure that one thread reading
# from a statement isn't affected by another thread starting a new query with
# the same statement.
# <p>
# Stability: Added in v1.00, stability guaranteed for v1.xx.
#
# @param connection     An instance of {@link Connection Connection}.
#
# @param statement      The SQL statement to be represented, often containing
# parameters in the form of $1, $2, $3, etc.
#
# @param types          Python type objects for each parameter in the SQL
# statement.  For example, int, float, str.
class PreparedStatement(object):

    ##
    # Determines the number of rows to read from the database server at once.
    # Reading more rows increases performance at the cost of memory.  The
    # default value is 100 rows.  The affect of this parameter is transparent.
    # That is, the library reads more rows when the cache is empty
    # automatically.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.  It is
    # possible that implementation changes in the future could cause this
    # parameter to be ignored.O
    row_cache_size = 100

    def __init__(self, connection, statement, *types):
        self.c = connection.c
        self._portal_name = "pg8000_portal_%s_%s" % (id(self.c), id(self))
        self._statement_name = "pg8000_statement_%s_%s" % (id(self.c), id(self))
        self._row_desc = None
        self._cached_rows = []
        self._command_complete = True
        self._parse_row_desc = self.c.parse(self._statement_name, statement, types)
        self._lock = threading.RLock()

    def __del__(self):
        # This __del__ should work with garbage collection / non-instant
        # cleanup.  It only really needs to be called right away if the same
        # object id (and therefore the same statement name) might be reused
        # soon, and clearly that wouldn't happen in a GC situation.
        self.c.close_statement(self._statement_name)

    row_description = property(lambda self: self._getRowDescription())
    def _getRowDescription(self):
        return self._row_desc.fields

    ##
    # Run the SQL prepared statement with the given parameters.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def execute(self, *args):
        self._lock.acquire()
        try:
            if not self._command_complete:
                # cleanup last execute
                self._cached_rows = []
                self.c.close_portal(self._portal_name)
            self._command_complete = False
            self._row_desc = self.c.bind(self._portal_name, self._statement_name, args, self._parse_row_desc)
            if self._row_desc:
                # We execute our cursor right away to fill up our cache.  This
                # prevents the cursor from being destroyed, apparently, by a rogue
                # Sync between Bind and Execute.  Since it is quite likely that
                # data will be read from us right away anyways, this seems a safe
                # move for now.
                self._fill_cache()
        finally:
            self._lock.release()

    def _fill_cache(self):
        self._lock.acquire()
        try:
            if self._cached_rows:
                raise InternalError("attempt to fill cache that isn't empty")
            end_of_data, rows = self.c.fetch_rows(self._portal_name, self.row_cache_size, self._row_desc)
            self._cached_rows = rows
            if end_of_data:
                self._command_complete = True
        finally:
            self._lock.release()

    def _fetch(self):
        self._lock.acquire()
        try:
            if not self._cached_rows:
                if self._command_complete:
                    return None
                self._fill_cache()
                if self._command_complete and not self._cached_rows:
                    # fill cache tells us the command is complete, but yet we have
                    # no rows after filling our cache.  This is a special case when
                    # a query returns no rows.
                    return None
            row = self._cached_rows[0]
            del self._cached_rows[0]
            return tuple(row)
        finally:
            self._lock.release()

    ##
    # Read a row from the database server, and return it in a dictionary
    # indexed by column name/alias.  This method will raise an error if two
    # columns have the same name.  Returns None after the last row.
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
    # Returns None after the last row.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def read_tuple(self):
        row = self._fetch()
        if row == None:
            return row
        return row

    ##
    # Return an iterator for the output of this statement.  The iterator will
    # return a tuple for each row, in the same manner as {@link
    # #PreparedStatement.read_tuple read_tuple}.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def iterate_tuple(self):
        return DataIterator(self, PreparedStatement.read_tuple)

    ##
    # Return an iterator for the output of this statement.  The iterator will
    # return a dict for each row, in the same manner as {@link
    # #PreparedStatement.read_dict read_dict}.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def iterate_dict(self):
        return DataIterator(self, PreparedStatement.read_dict)

##
# The Cursor class allows multiple queries to be performed concurrently with a
# single PostgreSQL connection.  The Cursor object is implemented internally by
# using a {@link PreparedStatement PreparedStatement} object, so if you plan to
# use a statement multiple times, you might as well create a PreparedStatement
# and save a small amount of reparsing time.
# <p>
# As of v1.01, instances of this class are thread-safe.  See {@link
# PreparedStatement PreparedStatement} for more information.
# <p>
# Stability: Added in v1.00, stability guaranteed for v1.xx.
#
# @param connection     An instance of {@link Connection Connection}.
class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self._stmt = None

    row_description = property(lambda self: self._getRowDescription())
    def _getRowDescription(self):
        if self._stmt == None:
            return None
        return self._stmt.row_description

    ##
    # Run an SQL statement using this cursor.  The SQL statement can have
    # parameters in the form of $1, $2, $3, etc., which will be filled in by
    # the additional arguments passed to this function.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    # @param query      The SQL statement to execute.
    def execute(self, query, *args):
        self._stmt = PreparedStatement(self.connection, query, *[type(x) for x in args])
        self._stmt.execute(*args)

    ##
    # Read a row from the database server, and return it in a dictionary
    # indexed by column name/alias.  This method will raise an error if two
    # columns have the same name.  Returns None after the last row.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def read_dict(self):
        if self._stmt == None:
            raise ProgrammingError("attempting to read from unexecuted cursor")
        return self._stmt.read_dict()

    ##
    # Read a row from the database server, and return it as a tuple of values.
    # Returns None after the last row.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def read_tuple(self):
        if self._stmt == None:
            raise ProgrammingError("attempting to read from unexecuted cursor")
        return self._stmt.read_tuple()

    ##
    # Return an iterator for the output of this statement.  The iterator will
    # return a tuple for each row, in the same manner as {@link
    # #PreparedStatement.read_tuple read_tuple}.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def iterate_tuple(self):
        if self._stmt == None:
            raise ProgrammingError("attempting to read from unexecuted cursor")
        return self._stmt.iterate_tuple()

    ##
    # Return an iterator for the output of this statement.  The iterator will
    # return a dict for each row, in the same manner as {@link
    # #PreparedStatement.read_dict read_dict}.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def iterate_dict(self):
        if self._stmt == None:
            raise ProgrammingError("attempting to read from unexecuted cursor")
        return self._stmt.iterate_dict()

##
# This class represents a connection to a PostgreSQL database.
# <p>
# The database connection is derived from the {@link #Cursor Cursor} class,
# which provides a default cursor for running queries.  It also provides
# transaction control via the 'begin', 'commit', and 'rollback' methods.
# Without beginning a transaction explicitly, all statements will autocommit to
# the database.
# <p>
# As of v1.01, instances of this class are thread-safe.  See {@link
# PreparedStatement PreparedStatement} for more information.
# <p>
# Stability: Added in v1.00, stability guaranteed for v1.xx.
#
# @param user   The username to connect to the PostgreSQL server with.  This
# parameter is required.
#
# @keyparam host   The hostname of the PostgreSQL server to connect with.
# Providing this parameter is necessary for TCP/IP connections.  One of either
# host, or unix_sock, must be provided.
#
# @keyparam unix_sock   The path to the UNIX socket to access the database
# through, for example, '/tmp/.s.PGSQL.5432'.  One of either unix_sock or host
# must be provided.  The port parameter will have no affect if unix_sock is
# provided.
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
#
# @keyparam socket_timeout  Socket connect timeout measured in seconds.
# Defaults to 60 seconds.
class Connection(Cursor):
    def __init__(self, user, host=None, unix_sock=None, port=5432, database=None, password=None, socket_timeout=60):
        self._row_desc = None
        try:
            self.c = Protocol.Connection(unix_sock=unix_sock, host=host, port=port, socket_timeout=socket_timeout)
            #self.c.connect()
            self.c.authenticate(user, password=password, database=database)
        except socket.error, e:
            raise InterfaceError("communication error", e)
        Cursor.__init__(self, self)
        self._begin = PreparedStatement(self, "BEGIN TRANSACTION")
        self._commit = PreparedStatement(self, "COMMIT TRANSACTION")
        self._rollback = PreparedStatement(self, "ROLLBACK TRANSACTION")

    ##
    # Begins a new transaction.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def begin(self):
        self._begin.execute()

    ##
    # Commits the running transaction.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def commit(self):
        self._commit.execute()

    ##
    # Rolls back the running transaction.
    # <p>
    # Stability: Added in v1.00, stability guaranteed for v1.xx.
    def rollback(self):
        self._rollback.execute()


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
                self.params.append(Types.pg_value(params[i], fc, client_encoding = client_encoding))
            self.out_fc = out_fc

        def serialize(self):
            val = self.portal + "\x00" + self.ps + "\x00"
            val = val + struct.pack("!h", len(self.in_fc))
            for fc in self.in_fc:
                val = val + struct.pack("!h", fc)
            val = val + struct.pack("!h", len(self.params))
            for param in self.params:
                if param == None:
                    # special case, NULL value
                    val = val + struct.pack("!i", -1)
                else:
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

    class ParameterDescription(object):
        def __init__(self, type_oids):
            self.type_oids = type_oids
        def createFromData(data):
            count = struct.unpack("!h", data[:2])[0]
            type_oids = struct.unpack("!" + "i"*count, data[2:])
            return Protocol.ParameterDescription(type_oids)
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
        def __init__(self, unix_sock=None, host=None, port=5432, socket_timeout=60):
            self._client_encoding = "ascii"
            if unix_sock == None and host != None:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            elif unix_sock != None:
                self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                raise ProgrammingError("one of host or unix_sock must be provided")
            self._sock.settimeout(socket_timeout)
            if unix_sock == None and host != None:
                self._sock.connect((host, port))
            elif unix_sock != None:
                self._sock.connect(unix_sock)
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
            msg = Protocol.message_types[message_code].createFromData(bytes)
            if isinstance(msg, Protocol.NoticeResponse):
                # ignore NoticeResponse
                return self._read_message()
            else:
                return msg

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

        def parse(self, statement, qs, types):
            self.verifyState("ready")
            self._sock_lock.acquire()
            try:
                type_info = [Types.pg_type_info(x) for x in types]
                param_types, param_fc = [x[0] for x in type_info], [x[1] for x in type_info] # zip(*type_info) -- fails on empty arr
                self._send(Protocol.Parse(statement, qs, param_types))
                self._send(Protocol.DescribePreparedStatement(statement))
                self._send(Protocol.Flush())
                while 1:
                    msg = self._read_message()
                    if isinstance(msg, Protocol.ParseComplete):
                        # ok, good.
                        pass
                    elif isinstance(msg, Protocol.ParameterDescription):
                        # well, we don't really care -- we're going to send whatever
                        # we want and let the database deal with it.  But thanks
                        # anyways!
                        pass
                    elif isinstance(msg, Protocol.NoData):
                        # We're not waiting for a row description.  Return
                        # something destinctive to let bind know that there is no
                        # output.
                        return (None, param_fc)
                    elif isinstance(msg, Protocol.RowDescription):
                        return (msg, param_fc)
                    elif isinstance(msg, Protocol.ErrorResponse):
                        raise msg.createException()
                    else:
                        raise InternalError("Unexpected response msg %r" % (msg))
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
                    output_fc = [Types.py_type_info(f) for f in row_desc.fields]
                self._send(Protocol.Bind(portal, statement, param_fc, params, output_fc, self._client_encoding))
                # We need to describe the portal after bind, since the return
                # format codes will be different (hopefully, always what we
                # requested).
                self._send(Protocol.DescribePortal(portal))
                self._send(Protocol.Flush())
                while 1:
                    msg = self._read_message()
                    if isinstance(msg, Protocol.BindComplete):
                        # good news everybody!
                        pass
                    elif isinstance(msg, Protocol.NoData):
                        # No data means we should execute this command right away.
                        self._send(Protocol.Execute(portal, 0))
                        self._send(Protocol.Sync())
                        exc = None
                        while 1:
                            msg = self._read_message()
                            if isinstance(msg, Protocol.CommandComplete):
                                # more good news!
                                pass
                            elif isinstance(msg, Protocol.ReadyForQuery):
                                if exc != None:
                                    raise exc
                                break
                            elif isinstance(msg, Protocol.ErrorResponse):
                                exc = msg.createException()
                            else:
                                raise InternalError("unexpected response")
                        return None
                    elif isinstance(msg, Protocol.RowDescription):
                        # Return the new row desc, since it will have the format
                        # types we asked for
                        return msg
                    elif isinstance(msg, Protocol.ErrorResponse):
                        raise msg.createException()
                    else:
                        raise InternalError("Unexpected response msg %r" % (msg))
            finally:
                self._sock_lock.release()

        def fetch_rows(self, portal, row_count, row_desc):
            self.verifyState("ready")
            self._sock_lock.acquire()
            try:
                self._send(Protocol.Execute(portal, row_count))
                self._send(Protocol.Flush())
                rows = []
                end_of_data = False
                while 1:
                    msg = self._read_message()
                    if isinstance(msg, Protocol.DataRow):
                        rows.append(
                                [Types.py_value(msg.fields[i], row_desc.fields[i], client_encoding=self._client_encoding)
                                    for i in range(len(msg.fields))]
                                )
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
            finally:
                self._sock_lock.release()

        def close_statement(self, statement):
            self.verifyState("ready")
            self._sock_lock.acquire()
            try:
                self._send(Protocol.ClosePreparedStatement(statement))
                self._send(Protocol.Sync())
                while 1:
                    msg = self._read_message()
                    if isinstance(msg, Protocol.CloseComplete):
                        # thanks!
                        pass
                    elif isinstance(msg, Protocol.ReadyForQuery):
                        return
                    elif isinstance(msg, Protocol.ErrorResponse):
                        raise msg.createException()
                    else:
                        raise InternalError("Unexpected response msg %r" % msg)
            finally:
                self._sock_lock.release()

        def close_portal(self, portal):
            self.verifyState("ready")
            self._sock_lock.acquire()
            try:
                self._send(Protocol.ClosePortal(portal))
                self._send(Protocol.Sync())
                while 1:
                    msg = self._read_message()
                    if isinstance(msg, Protocol.CloseComplete):
                        # thanks!
                        pass
                    elif isinstance(msg, Protocol.ReadyForQuery):
                        return
                    elif isinstance(msg, Protocol.ErrorResponse):
                        raise msg.createException()
                    else:
                        raise InternalError("Unexpected response msg %r" % msg)
            finally:
                self._sock_lock.release()

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

class Bytea(str):
    pass

class Types(object):

    def pg_type_info(typ):
        data = Types.py_types.get(typ)
        if data == None:
            raise NotSupportedError("type %r not mapped to pg type" % typ)
        type_oid = data.get("tid")
        if type_oid == None:
            raise InternalError("type %r has no type_oid" % typ)
        elif type_oid == -1:
            # special case: NULL values
            return type_oid, 0
        prefer = data.get("prefer")
        if prefer != None:
            if prefer == "bin":
                if data.get("bin_out") == None:
                    raise InternalError("bin format prefered but not avail for type %r" % typ)
                format = 1
            elif prefer == "txt":
                if data.get("txt_out") == None:
                    raise InternalError("txt format prefered but not avail for type %r" % typ)
                format = 0
            else:
                raise InternalError("prefer flag not recognized for type %r" % typ)
        else:
            # by default, prefer bin, but go with whatever exists
            if data.get("bin_out"):
                format = 1
            elif data.get("txt_out"):
                format = 0
            else:
                raise InternalError("no conversion fuction for type %r" % typ)
        return type_oid, format
    pg_type_info = staticmethod(pg_type_info)

    def pg_value(v, fc, **kwargs):
        typ = type(v)
        data = Types.py_types.get(typ)
        if data == None:
            raise NotSupportedError("type %r not mapped to pg type" % typ)
        elif data.get("tid") == -1:
            # special case: NULL values
            return None
        if fc == 0:
            func = data.get("txt_out")
        elif fc == 1:
            func = data.get("bin_out")
        else:
            raise InternalError("unrecognized format code %r" % fc)
        if func == None:
            raise NotSupportedError("type %r, format code %r not supported" % (typ, fc))
        return func(v, **kwargs)
    pg_value = staticmethod(pg_value)

    def py_type_info(description):
        type_oid = description['type_oid']
        data = Types.pg_types.get(type_oid)
        if data == None:
            raise NotSupportedError("type oid %r not mapped to py type" % type_oid)
        prefer = data.get("prefer")
        if prefer != None:
            if prefer == "bin":
                if data.get("bin_in") == None:
                    raise InternalError("bin format prefered but not avail for type oid %r" % type_oid)
                format = 1
            elif prefer == "txt":
                if data.get("txt_in") == None:
                    raise InternalError("txt format prefered but not avail for type oid %r" % type_oid)
                format = 0
            else:
                raise InternalError("prefer flag not recognized for type oid %r" % type_oid)
        else:
            # by default, prefer bin, but go with whatever exists
            if data.get("bin_in"):
                format = 1
            elif data.get("txt_in"):
                format = 0
            else:
                raise InternalError("no conversion fuction for type oid %r" % type_oid)
        return format
    py_type_info = staticmethod(py_type_info)

    def py_value(v, description, **kwargs):
        if v == None:
            # special case - NULL value
            return None
        type_oid = description['type_oid']
        format = description['format']
        data = Types.pg_types.get(type_oid)
        if data == None:
            raise NotSupportedError("type oid %r not supported" % type_oid)
        if format == 0:
            func = data.get("txt_in")
        elif format == 1:
            func = data.get("bin_in")
        else:
            raise NotSupportedError("format code %r not supported" % format)
        if func == None:
            raise NotSupportedError("data response format %r, type %r not supported" % (format, type_oid))
        return func(v, **kwargs)
    py_value = staticmethod(py_value)

    def boolin(data, **kwargs):
        return data == 't'

    def boolrecv(data, **kwargs):
        return data == "\x01"

    def int2recv(data, **kwargs):
        return struct.unpack("!h", data)[0]

    def int2in(data, **kwargs):
        return int(data)

    def int4recv(data, **kwargs):
        return struct.unpack("!i", data)[0]

    def int4in(data, **kwargs):
        return int(data)

    def int8recv(data, **kwargs):
        return struct.unpack("!q", data)[0]

    def int8in(data, **kwargs):
        return int(data)

    def float4in(data, **kwargs):
        return float(data)

    def float4recv(data, **kwargs):
        return struct.unpack("!f", data)[0]

    def float8recv(data, **kwargs):
        return struct.unpack("!d", data)[0]

    def float8in(data, **kwargs):
        return float(data)

    def float8send(v, **kwargs):
        return struct.pack("!d", v)

    # The timestamp_recv function is sadly not in use because some PostgreSQL
    # servers are compiled with HAVE_INT64_TIMESTAMP, and some are not.  This
    # alters the binary format of the timestamp, cannot be perfectly detected,
    # and there is no message from the server indicating which format is in
    # use.  Ah, well, obviously binary formats are hit-and-miss...
    #def timestamp_recv(data, **kwargs):
    #    val = struct.unpack("!d", data)[0]
    #    return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds = val)

    def timestamp_in(data, **kwargs):
        year = int(data[0:4])
        month = int(data[5:7])
        day = int(data[8:10])
        hour = int(data[11:13])
        minute = int(data[14:16])
        sec = decimal.Decimal(data[17:])
        return datetime.datetime(year, month, day, hour, minute, int(sec), int((sec - int(sec)) * 1000000))

    def numeric_in(data, **kwargs):
        if data.find(".") == -1:
            return int(data)
        else:
            return decimal.Decimal(data)

    def numeric_out(v, **kwargs):
        return str(v)

    def varcharin(data, client_encoding, **kwargs):
        return unicode(data, client_encoding)

    def textout(v, client_encoding, **kwargs):
        return v.encode(client_encoding)

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

    def byteasend(v, **kwargs):
        return str(v)

    def bytearecv(data, **kwargs):
        return Bytea(data)

    # interval support does not provide a Python-usable interval object yet
    def interval_in(data, **kwargs):
        return data

    py_types = {
        int: {"tid": 1700, "txt_out": numeric_out},
        long: {"tid": 1700, "txt_out": numeric_out},
        str: {"tid": 25, "txt_out": textout},
        unicode: {"tid": 25, "txt_out": textout},
        float: {"tid": 701, "bin_out": float8send},
        decimal.Decimal: {"tid": 1700, "txt_out": numeric_out},
        Bytea: {"tid": 17, "bin_out": byteasend},
        type(None): {"tid": -1},
    }

    pg_types = {
        16: {"txt_in": boolin, "bin_in": boolrecv, "prefer": "bin"},
        17: {"bin_in": bytearecv},
        20: {"txt_in": int8in, "bin_in": int8recv, "prefer": "bin"},
        21: {"txt_in": int2in, "bin_in": int2recv, "prefer": "bin"},
        23: {"txt_in": int4in, "bin_in": int4recv, "prefer": "bin"},
        25: {"txt_in": varcharin}, # TEXT type
        26: {"txt_in": numeric_in}, # oid type
        700: {"txt_in": float4in, "bin_in": float4recv, "prefer": "bin"},
        701: {"txt_in": float8in, "bin_in": float8recv, "prefer": "bin"},
        1042: {"txt_in": varcharin}, # CHAR type
        1043: {"txt_in": varcharin}, # VARCHAR type
        1114: {"txt_in": timestamp_in}, #, "bin_in": timestamp_recv, "prefer": "bin"},
        1186: {"txt_in": interval_in},
        1700: {"txt_in": numeric_in},
    }
        #1184: (timestamptz_in, None), # timestamp w/ tz



