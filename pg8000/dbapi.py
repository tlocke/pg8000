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

from . import util, errors, interface, types
from .errors import *
import datetime
import itertools
import time
import threading

##
# The DBAPI level supported.  Currently 2.0.  This property is part of the
# DBAPI 2.0 specification.
apilevel = "2.0"

##
# Integer constant stating the level of thread safety the DBAPI interface
# supports.  This DBAPI interface supports sharing of the module, connections,
# and cursors.  This property is part of the DBAPI 2.0 specification.
threadsafety = 3

##
# String property stating the type of parameter marker formatting expected by
# the interface.  This value defaults to "format".  This property is part of
# the DBAPI 2.0 specification.
# <p>
# Unlike the DBAPI specification, this value is not constant.  It can be
# changed to any standard paramstyle value (ie. qmark, numeric, named, format,
# and pyformat).
paramstyle = 'pyformat'  # paramstyle can be changed to any DB-API paramstyle


def require_open_cursor(fn):
    def _fn(self, *args, **kwargs):
        if self.cursor == None:
            raise CursorClosedError()
        return fn(self, *args, **kwargs)
    return _fn

##
# The class of object returned by the
# {@link #ConnectionWrapper.cursor cursor method}.
class CursorWrapper(object):
    def __init__(self, conn, connection):
        self.cursor = interface.Cursor(conn)
        self.arraysize = 1
        self._connection = connection
        self._override_rowcount = None
        self._row_adapter = None

    ##
    # This read-only attribute returns a reference to the connection object on
    # which the cursor was created.
    # <p>
    # Stability: Part of a DBAPI 2.0 extension.  A warning "DB-API extension
    # cursor.connection used" will be fired.
    connection = property(lambda self: self._getConnection())

    def _getConnection(self):
        return self._connection

    ##
    # This read-only attribute specifies the number of rows that the last
    # .execute*() produced (for DQL statements like 'select') or affected (for
    # DML statements like 'update' or 'insert').
    # <p>
    # The attribute is -1 in case no .execute*() has been performed on the
    # cursor or the rowcount of the last operation is cannot be determined by
    # the interface.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @property
    @require_open_cursor
    def rowcount(self):
        if self._override_rowcount != None:
            return self._override_rowcount
        return self.cursor.row_count

    ##
    # This read-only attribute is a sequence of 7-item sequences.  Each value
    # contains information describing one result column.  The 7 items returned
    # for each column are (name, type_code, display_size, internal_size,
    # precision, scale, null_ok).  Only the first two values are provided by
    # this interface implementation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @property
    @require_open_cursor
    def description(self):
        if self.cursor.row_description == None:
            return None
        columns = []
        for col in self.cursor.row_description:
            columns.append((col["name"], col["type_oid"],
                                None, None, None, None, None))
        return columns

    ##
    # Executes a database operation.  Parameters may be provided as a sequence
    # or mapping and will be bound to variables in the operation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_cursor
    def execute(self, operation, args=()):
        if not self._connection.in_transaction:
            self._connection.begin()
        self._override_rowcount = None
        if hasattr(args, 'keys'):
            query, param_fn = util.coerce_named(operation, args)
        else:
            query, param_fn = util.coerce_positional(operation, args)
        self._execute(query, param_fn(args))

    ##
    # Prepare a database operation and then execute it against all parameter
    # sequences or mappings provided.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_cursor
    def executemany(self, operation, parameter_sets):
        if not self._connection.in_transaction:
            self._connection.begin()
        self._override_rowcount = 0

        query = None
        for parameters in parameter_sets:
            if not query:
                if hasattr(parameters, 'keys'):
                    query, param_fn = util.coerce_named(operation, parameters)
                else:
                    query, param_fn = util.coerce_positional(operation, parameters)

            self._execute(query, param_fn(parameters))
            if self.cursor.row_count == -1 or self._override_rowcount == -1:
                self._override_rowcount = -1
            else:
                self._override_rowcount += self.cursor.row_count

    def _execute(self, operation, args):
        try:
            self.cursor.execute(operation, row_adapter=self._row_adapter, *args)
        except ConnectionClosedError:
            # can't rollback in this case
            raise
        except:
            # any error will rollback the transaction to-date
            self._connection.rollback()
            raise

    def copy_from(self, fileobj, table=None, sep='\t', null=None, query=None):
        if query == None:
            if table == None:
                raise CopyQueryOrTableRequiredError()
            query = "COPY %s FROM stdout DELIMITER '%s'" % (table, sep)
            if null is not None:
                query += " NULL '%s'" % (null,)
        self.copy_execute(fileobj, query)

    def copy_to(self, fileobj, table=None, sep='\t', null=None, query=None):
        if query == None:
            if table == None:
                raise CopyQueryOrTableRequiredError()
            query = "COPY %s TO stdout DELIMITER '%s'" % (table, sep)
            if null is not None:
                query += " NULL '%s'" % (null,)
        self.copy_execute(fileobj, query)

    @require_open_cursor
    def copy_execute(self, fileobj, query):
        try:
            self.cursor.execute(query, stream=fileobj)
        except ConnectionClosedError:
            # can't rollback in this case
            raise
        except:
            # any error will rollback the transaction to-date
            import traceback
            traceback.print_exc()
            self._connection.rollback()
            raise


    ##
    # Fetch the next row of a query result set, returning a single sequence, or
    # None when no more data is available.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_cursor
    def fetchone(self):
        return self.cursor.read_tuple()

    ##
    # Fetch the next set of rows of a query result, returning a sequence of
    # sequences.  An empty sequence is returned when no more rows are
    # available.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    # @param size   The number of rows to fetch when called.  If not provided,
    #               the arraysize property value is used instead.
    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        def iter():
            for i, row in enumerate(self.cursor.iterate_tuple()):
                yield row
                if i >= size - 1:
                    break
        return list(iter())

    ##
    # Fetch all remaining rows of a query result, returning them as a sequence
    # of sequences.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_cursor
    def fetchall(self):
        return list(self.cursor.iterate_tuple())

    ##
    # Close the cursor.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_cursor
    def close(self):
        self.cursor.close()
        self.cursor = None
        self._override_rowcount = None

    def next(self):
        retval = self.fetchone()
        if retval == None:
            raise StopIteration()
        return retval

    def __iter__(self):
        return self

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    @require_open_cursor
    def fileno(self):
        return self.cursor.fileno()

    @require_open_cursor
    def isready(self):
        return self.cursor.isready()

def require_open_connection(fn):
    def _fn(self, *args, **kwargs):
        if self.conn == None:
            raise ConnectionClosedError()
        return fn(self, *args, **kwargs)
    return _fn

##
# The class of object returned by the {@link #connect connect method}.
class ConnectionWrapper(object):

    # DBAPI Extension: supply exceptions as
    # attributes on the connection
    Warning = errors.Warning
    Error = errors.Error
    InterfaceError = errors.InterfaceError
    DatabaseError = errors.DatabaseError
    OperationalError = errors.OperationalError
    IntegrityError = errors.IntegrityError
    InternalError = errors.InternalError
    ProgrammingError = errors.ProgrammingError
    NotSupportedError = errors.NotSupportedError

    @property
    def in_transaction(self):
        if self.conn:
            return self.conn.in_transaction
        return False

    def __init__(self, **kwargs):
        self.conn = interface.Connection(**kwargs)
        self.notifies = []
        self.notifies_lock = threading.Lock()
        self.conn.NotificationReceived += self._notificationReceived
        self._extensions = []

    def enable_extension(self, ext):
        ext.enable_extension(self)

    def disable_extension(self, ext):
        ext.disable_extension(self)

    @require_open_connection
    def begin(self):
        self.conn.begin()

    def _notificationReceived(self, notice):
        try:
        # psycopg2 compatible notification interface
            self.notifies_lock.acquire()
            self.notifies.append((notice.backend_pid, notice.condition))
        finally:
            self.notifies_lock.release()

    ##
    # Creates a {@link #CursorWrapper CursorWrapper} object bound to this
    # connection.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_connection
    def cursor(self):
        cursor = CursorWrapper(self.conn, self)
        for ext in self._extensions:
            ext.new_cursor(self, cursor)
        return cursor

    ##
    # Commits the current database transaction.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_connection
    def commit(self):
        # There's a threading bug here.  If a query is sent after the
        # commit, but before the begin, it will be executed immediately
        # without a surrounding transaction.  Like all threading bugs -- it
        # sounds unlikely, until it happens every time in one
        # application...  however, to fix this, we need to lock the
        # database connection entirely, so that no cursors can execute
        # statements on other threads.  Support for that type of lock will
        # be done later.
        self.conn.commit()

    ##
    # Rolls back the current database transaction.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_connection
    def rollback(self):
        # see bug description in commit.
        self.conn.rollback()

    ##
    # Closes the database connection.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    @require_open_connection
    def close(self):
        self.conn.close()
        self.conn = None

    ##
    # Returns the "server_version" string provided by the connected server.
    # <p>
    # Stability: Extension of the DBAPI 2.0 specification.
    @property
    @require_open_connection
    def server_version(self):
        return self.conn.server_version()

##
# Creates a DBAPI 2.0 compatible interface to a PostgreSQL database.
# <p>
# Stability: Part of the DBAPI 2.0 specification.
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
#
# @keyparam ssl     Use SSL encryption for TCP/IP socket.  Defaults to False.
#
# @return An instance of {@link #ConnectionWrapper ConnectionWrapper}.
def connect(user, host=None, unix_sock=None, port=5432, database=None,
                    password=None, socket_timeout=60, ssl=False):
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

##
# Construct an object holding binary data.
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


