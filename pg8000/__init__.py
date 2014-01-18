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


exec("from struct import Struct")
for fmt in (
        "i", "h", "q", "d", "f", "iii", "ii", "qii", "dii", "ihihih", "ci",
        "bh", "cccc"):
    exec(fmt + "_struct = Struct('!" + fmt + "')")
    exec(fmt + "_unpack = " + fmt + "_struct.unpack_from")
    exec(fmt + "_pack = " + fmt + "_struct.pack")

import datetime
import time
from pg8000.six import binary_type, integer_types, PY2

min_int2, max_int2 = -2 ** 15, 2 ** 15
min_int4, max_int4 = -2 ** 31, 2 ** 31
min_int8, max_int8 = -2 ** 63, 2 ** 63


class Bytea(binary_type):
    pass


class Interval(object):
    def __init__(self, microseconds=0, days=0, months=0):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def _setMicroseconds(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("microseconds must be an integer type")
        elif not (min_int8 < value < max_int8):
            raise OverflowError(
                "microseconds must be representable as a 64-bit integer")
        else:
            self._microseconds = value

    def _setDays(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("days must be an integer type")
        elif not (min_int4 < value < max_int4):
            raise OverflowError(
                "days must be representable as a 32-bit integer")
        else:
            self._days = value

    def _setMonths(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("months must be an integer type")
        elif not (min_int4 < value < max_int4):
            raise OverflowError(
                "months must be representable as a 32-bit integer")
        else:
            self._months = value

    microseconds = property(lambda self: self._microseconds, _setMicroseconds)
    days = property(lambda self: self._days, _setDays)
    months = property(lambda self: self._months, _setMonths)

    def __repr__(self):
        return "<Interval %s months %s days %s microseconds>" % (
            self.months, self.days, self.microseconds)

    def __eq__(self, other):
        return other is not None and isinstance(other, Interval) and \
            self.months == other.months and self.days == other.days and \
            self.microseconds == other.microseconds

    def __neq__(self, other):
        return not self.__eq__(other)

import pg8000.core


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
def connect(
        user, host='localhost', unix_sock=None, port=5432, database=None,
        password=None, socket_timeout=60, ssl=False):
    return pg8000.core.Connection(
        user, host, unix_sock, port, database, password, socket_timeout, ssl)

##
# The DBAPI level supported.  Currently 2.0.  This property is part of the
# DBAPI 2.0 specification.
apilevel = "2.0"

##
# Integer constant stating the level of thread safety the DBAPI interface
# supports.  This DBAPI interface supports sharing of the module and
# connections.  This property is part of the DBAPI 2.0 specification.
threadsafety = 3

##
# String property stating the type of parameter marker formatting expected by
# the interface.  This value defaults to "format".  This property is part of
# the DBAPI 2.0 specification.
# <p>
# Unlike the DBAPI specification, this value is not constant.  It can be
# changed to any standard paramstyle value (ie. qmark, numeric, named, format,
# and pyformat).
paramstyle = 'format'  # paramstyle can be changed to any DB-API paramstyle

# I have no idea what this would be used for by a client app.  Should it be
# TEXT, VARCHAR, CHAR?  It will only compare against row_description's
# type_code if it is this one type.  It is the varchar type oid for now, this
# appears to match expectations in the DB API 2.0 compliance test suite.

STRING = 1043

if PY2:
    BINARY = Bytea
else:
    BINARY = bytes

# numeric type_oid
NUMBER = 1700

# timestamp type_oid
DATETIME = 1114

# oid type_oid
ROWID = 26


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


# Construct an object holding binary data.
def Binary(value):
    if PY2:
        return Bytea(value)
    else:
        return value

try:
    from pytz import utc
except ImportError:
    ZERO = datetime.timedelta(0)

    class UTC(datetime.tzinfo):

        def utcoffset(self, dt):
            return ZERO

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return ZERO
    utc = UTC()


# For compatibility with 1.8
import pg8000 as dbapi
DBAPI = dbapi
pg8000_dbapi = DBAPI

from pg8000.errors import (
    Warning, DatabaseError, InterfaceError,
    ProgrammingError, CopyQueryOrTableRequiredError, Error, OperationalError,
    IntegrityError, InternalError, NotSupportedError,
    ArrayContentNotHomogenousError, ArrayContentEmptyError,
    ArrayDimensionsNotConsistentError, ArrayContentNotSupportedError)

__all__ = [
    Warning, Bytea, DatabaseError, connect, InterfaceError, ProgrammingError,
    CopyQueryOrTableRequiredError, Error, OperationalError, IntegrityError,
    InternalError, NotSupportedError, ArrayContentNotHomogenousError,
    ArrayContentEmptyError, ArrayDimensionsNotConsistentError,
    ArrayContentNotSupportedError]
