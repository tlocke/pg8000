from datetime import (
    timedelta as Timedelta, datetime as Datetime, date, time)
from warnings import warn
import socket
from hashlib import md5
from decimal import Decimal
from collections import deque, defaultdict
from itertools import count, islice
from distutils.version import LooseVersion
import pg8000
from scramp import ScramClient
import enum
from ipaddress import IPv4Address, IPv6Address, IPv4Network, IPv6Network
from pg8000 import converters
from uuid import UUID
from pg8000.exceptions import (
    InterfaceError, ProgrammingError, Error, DatabaseError, OperationalError,
    IntegrityError, InternalError, NotSupportedError, Warning,
    ArrayContentNotSupportedError, ArrayDimensionsNotConsistentError)
from struct import Struct


def pack_funcs(fmt):
    struc = Struct('!' + fmt)
    return struc.pack, struc.unpack_from


i_pack, i_unpack = pack_funcs('i')
h_pack, h_unpack = pack_funcs('h')
ii_pack, ii_unpack = pack_funcs('ii')
ihihih_pack, ihihih_unpack = pack_funcs('ihihih')
ci_pack, ci_unpack = pack_funcs('ci')
bh_pack, bh_unpack = pack_funcs('bh')
cccc_pack, cccc_unpack = pack_funcs('cccc')


# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
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


NULL = i_pack(-1)

'''
FC_TEXT = 0
FC_BINARY = 1
'''

min_int2, max_int2 = -2 ** 15, 2 ** 15
min_int4, max_int4 = -2 ** 31, 2 ** 31
min_int8, max_int8 = -2 ** 63, 2 ** 63


def convert_paramstyle(style, query):
    # I don't see any way to avoid scanning the query string char by char,
    # so we might as well take that careful approach and create a
    # state-based scanner.  We'll use int variables for the state.
    OUTSIDE = 0    # outside quoted string
    INSIDE_SQ = 1  # inside single-quote string '...'
    INSIDE_QI = 2  # inside quoted identifier   "..."
    INSIDE_ES = 3  # inside escaped single-quote string, E'...'
    INSIDE_PN = 4  # inside parameter name eg. :name
    INSIDE_CO = 5  # inside inline comment eg. --

    in_quote_escape = False
    in_param_escape = False
    placeholders = []
    output_query = []
    param_idx = map(lambda x: "$" + str(x), count(1))
    state = OUTSIDE
    prev_c = None
    for i, c in enumerate(query):
        if i + 1 < len(query):
            next_c = query[i + 1]
        else:
            next_c = None

        if state == OUTSIDE:
            if c == "'":
                output_query.append(c)
                if prev_c == 'E':
                    state = INSIDE_ES
                else:
                    state = INSIDE_SQ
            elif c == '"':
                output_query.append(c)
                state = INSIDE_QI
            elif c == '-':
                output_query.append(c)
                if prev_c == '-':
                    state = INSIDE_CO
            elif style == "qmark" and c == "?":
                output_query.append(next(param_idx))
            elif style == "numeric" and c == ":" and next_c not in ':=' \
                    and prev_c != ':':
                # Treat : as beginning of parameter name if and only
                # if it's the only : around
                # Needed to properly process type conversions
                # i.e. sum(x)::float
                output_query.append("$")
            elif style == "named" and c == ":" and next_c not in ':=' \
                    and prev_c != ':':
                # Same logic for : as in numeric parameters
                state = INSIDE_PN
                placeholders.append('')
            elif style == "pyformat" and c == '%' and next_c == "(":
                state = INSIDE_PN
                placeholders.append('')
            elif style in ("format", "pyformat") and c == "%":
                style = "format"
                if in_param_escape:
                    in_param_escape = False
                    output_query.append(c)
                else:
                    if next_c == "%":
                        in_param_escape = True
                    elif next_c == "s":
                        state = INSIDE_PN
                        output_query.append(next(param_idx))
                    else:
                        raise InterfaceError(
                            "Only %s and %% are supported in the query.")
            else:
                output_query.append(c)

        elif state == INSIDE_SQ:
            if c == "'":
                if in_quote_escape:
                    in_quote_escape = False
                else:
                    if next_c == "'":
                        in_quote_escape = True
                    else:
                        state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_QI:
            if c == '"':
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_ES:
            if c == "'" and prev_c != "\\":
                # check for escaped single-quote
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_PN:
            if style == 'named':
                placeholders[-1] += c
                if next_c is None or (not next_c.isalnum() and next_c != '_'):
                    state = OUTSIDE
                    try:
                        pidx = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
            elif style == 'pyformat':
                if prev_c == ')' and c == "s":
                    state = OUTSIDE
                    try:
                        pidx = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
                elif c in "()":
                    pass
                else:
                    placeholders[-1] += c
            elif style == 'format':
                state = OUTSIDE

        elif state == INSIDE_CO:
            output_query.append(c)
            if c == '\n':
                state = OUTSIDE

        prev_c = c

    if style in ('numeric', 'qmark', 'format'):
        def make_args(vals):
            return vals
    else:
        def make_args(vals):
            return tuple(vals[p] for p in placeholders)

    return ''.join(output_query), make_args


NULL_BYTE = b'\x00'


class Cursor():
    """A cursor object is returned by the :meth:`~Connection.cursor` method of
    a connection. It has the following attributes and methods:

    .. attribute:: arraysize

        This read/write attribute specifies the number of rows to fetch at a
        time with :meth:`fetchmany`.  It defaults to 1.

    .. attribute:: connection

        This read-only attribute contains a reference to the connection object
        (an instance of :class:`Connection`) on which the cursor was
        created.

        This attribute is part of a DBAPI 2.0 extension.  Accessing this
        attribute will generate the following warning: ``DB-API extension
        cursor.connection used``.

    .. attribute:: rowcount

        This read-only attribute contains the number of rows that the last
        ``execute()`` or ``executemany()`` method produced (for query
        statements like ``SELECT``) or affected (for modification statements
        like ``UPDATE``).

        The value is -1 if:

        - No ``execute()`` or ``executemany()`` method has been performed yet
          on the cursor.
        - There was no rowcount associated with the last ``execute()``.
        - At least one of the statements executed as part of an
          ``executemany()`` had no row count associated with it.
        - Using a ``SELECT`` query statement on PostgreSQL server older than
          version 9.
        - Using a ``COPY`` query statement on PostgreSQL server version 8.1 or
          older.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. attribute:: description

        This read-only attribute is a sequence of 7-item sequences.  Each value
        contains information describing one result column.  The 7 items
        returned for each column are (name, type_code, display_size,
        internal_size, precision, scale, null_ok).  Only the first two values
        are provided by the current implementation.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self, connection, paramstyle=None):
        self._c = connection
        self.arraysize = 1
        self._row_count = -1
        self._cached_rows = deque()
        self.row_desc = None
        if paramstyle is None:
            self.paramstyle = pg8000.paramstyle
        else:
            self.paramstyle = paramstyle

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def connection(self):
        warn("DB-API extension cursor.connection used", stacklevel=3)
        return self._c

    @property
    def rowcount(self):
        return self._row_count

    description = property(lambda self: self._getDescription())

    def _getDescription(self):
        row_desc = self.row_desc
        if row_desc is None:
            return None
        if len(row_desc) == 0:
            return None
        columns = []
        for col in row_desc:
            columns.append(
                (col["name"], col["type_oid"], None, None, None, None, None))
        return columns

    ##
    # Executes a database operation.  Parameters may be provided as a sequence
    # or mapping and will be bound to variables in the operation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    def execute(self, operation, args=None, stream=None):
        """Executes a database operation.  Parameters may be provided as a
        sequence, or as a mapping, depending upon the value of
        :data:`pg8000.paramstyle`.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute.

        :param args:
            If :data:`paramstyle` is ``qmark``, ``numeric``, or ``format``,
            this argument should be an array of parameters to bind into the
            statement.  If :data:`paramstyle` is ``named``, the argument should
            be a dict mapping of parameters.  If the :data:`paramstyle` is
            ``pyformat``, the argument value may be either an array or a
            mapping.

        :param stream: This is a pg8000 extension for use with the PostgreSQL
            `COPY
            <http://www.postgresql.org/docs/current/static/sql-copy.html>`_
            command. For a COPY FROM the parameter must be a readable file-like
            object, and for COPY TO it must be writable.

            .. versionadded:: 1.9.11
        """
        try:
            self.stream = stream

            if not self._c.in_transaction and not self._c.autocommit:
                self._c.execute_unnamed(self, "begin transaction", None)
            self._c.execute_unnamed(self, operation, args)
        except AttributeError as e:
            if self._c is None:
                raise InterfaceError("Cursor closed")
            elif self._c._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e
        return self

    def executemany(self, operation, param_sets):
        """Prepare a database operation, and then execute it against all
        parameter sequences or mappings provided.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute
        :param parameter_sets:
            A sequence of parameters to execute the statement with. The values
            in the sequence should be sequences or mappings of parameters, the
            same as the args argument of the :meth:`execute` method.
        """
        rowcounts = []
        for parameters in param_sets:
            self.execute(operation, parameters)
            rowcounts.append(self._row_count)

        self._row_count = -1 if -1 in rowcounts else sum(rowcounts)
        return self

    def fetchone(self):
        """Fetch the next row of a query result set.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:
            A row as a sequence of field values, or ``None`` if no more rows
            are available.
        """
        try:
            return next(self)
        except StopIteration:
            return None
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")
        except AttributeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchmany(self, num=None):
        """Fetches the next set of rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param size:

            The number of rows to fetch when called.  If not provided, the
            :attr:`arraysize` attribute value is used instead.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.  If no more rows are available, an empty sequence
            will be returned.
        """
        try:
            return tuple(
                islice(self, self.arraysize if num is None else num))
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchall(self):
        """Fetches all remaining rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.
        """
        try:
            return tuple(self)
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def close(self):
        """Closes the cursor.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._c = None

    def __iter__(self):
        """A cursor object is iterable to retrieve the rows from a query.

        This is a DBAPI 2.0 extension.
        """
        return self

    def setinputsizes(self, sizes):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented by pg8000.
        """
        pass

    def setoutputsize(self, size, column=None):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented by pg8000.
        """
        pass

    def __next__(self):
        try:
            return self._cached_rows.popleft()
        except IndexError:
            if self.row_desc is None:
                raise ProgrammingError("A query hasn't been issued.")
            elif len(self.row_desc) == 0:
                raise ProgrammingError("no result set")
            else:
                raise StopIteration()


# Message codes
NOTICE_RESPONSE = b"N"
AUTHENTICATION_REQUEST = b"R"
PARAMETER_STATUS = b"S"
BACKEND_KEY_DATA = b"K"
READY_FOR_QUERY = b"Z"
ROW_DESCRIPTION = b"T"
ERROR_RESPONSE = b"E"
DATA_ROW = b"D"
COMMAND_COMPLETE = b"C"
PARSE_COMPLETE = b"1"
BIND_COMPLETE = b"2"
CLOSE_COMPLETE = b"3"
PORTAL_SUSPENDED = b"s"
NO_DATA = b"n"
PARAMETER_DESCRIPTION = b"t"
NOTIFICATION_RESPONSE = b"A"
COPY_DONE = b"c"
COPY_DATA = b"d"
COPY_IN_RESPONSE = b"G"
COPY_OUT_RESPONSE = b"H"
EMPTY_QUERY_RESPONSE = b"I"

BIND = b"B"
PARSE = b"P"
EXECUTE = b"E"
FLUSH = b'H'
SYNC = b'S'
PASSWORD = b'p'
DESCRIBE = b'D'
TERMINATE = b'X'
CLOSE = b'C'


def create_message(code, data=b''):
    return code + i_pack(len(data) + 4) + data


FLUSH_MSG = create_message(FLUSH)
SYNC_MSG = create_message(SYNC)
TERMINATE_MSG = create_message(TERMINATE)
COPY_DONE_MSG = create_message(COPY_DONE)
EXECUTE_MSG = create_message(EXECUTE, NULL_BYTE + i_pack(0))

# DESCRIBE constants
STATEMENT = b'S'
PORTAL = b'P'

# ErrorResponse codes
RESPONSE_SEVERITY = "S"  # always present
RESPONSE_SEVERITY = "V"  # always present
RESPONSE_CODE = "C"  # always present
RESPONSE_MSG = "M"  # always present
RESPONSE_DETAIL = "D"
RESPONSE_HINT = "H"
RESPONSE_POSITION = "P"
RESPONSE__POSITION = "p"
RESPONSE__QUERY = "q"
RESPONSE_WHERE = "W"
RESPONSE_FILE = "F"
RESPONSE_LINE = "L"
RESPONSE_ROUTINE = "R"

IDLE = b"I"
IDLE_IN_TRANSACTION = b"T"
IDLE_IN_FAILED_TRANSACTION = b"E"


arr_trans = dict(zip(map(ord, "[] '"), list('{}') + [None] * 2))


class Connection():

    # DBAPI Extension: supply exceptions as attributes on the connection
    Warning = property(lambda self: self._getError(Warning))
    Error = property(lambda self: self._getError(Error))
    InterfaceError = property(lambda self: self._getError(InterfaceError))
    DatabaseError = property(lambda self: self._getError(DatabaseError))
    OperationalError = property(lambda self: self._getError(OperationalError))
    IntegrityError = property(lambda self: self._getError(IntegrityError))
    InternalError = property(lambda self: self._getError(InternalError))
    ProgrammingError = property(lambda self: self._getError(ProgrammingError))
    NotSupportedError = property(
        lambda self: self._getError(NotSupportedError))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _getError(self, error):
        warn(
            "DB-API extension connection.%s used" %
            error.__name__, stacklevel=3)
        return error

    def __init__(
            self, user, host='localhost', database=None, port=5432,
            password=None, source_address=None, unix_sock=None,
            ssl_context=None, timeout=None, tcp_keepalive=True,
            application_name=None, replication=None):
        self._client_encoding = "utf8"
        self._commands_with_count = (
            b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH", b"COPY",
            b"SELECT")
        self.notifications = deque(maxlen=100)
        self.notices = deque(maxlen=100)
        self.parameter_statuses = deque(maxlen=100)
        self._run_cursor = Cursor(self, paramstyle='named')

        if user is None:
            raise InterfaceError(
                "The 'user' connection parameter cannot be None")

        init_params = {
            'user': user,
            'database': database,
            'application_name': application_name,
            'replication': replication
        }

        for k, v in tuple(init_params.items()):
            if isinstance(v, str):
                init_params[k] = v.encode('utf8')
            elif v is None:
                del init_params[k]
            elif not isinstance(v, (bytes, bytearray)):
                raise InterfaceError(
                    "The parameter " + k + " can't be of type " +
                    str(type(v)) + ".")

        self.user = init_params['user']

        if isinstance(password, str):
            self.password = password.encode('utf8')
        else:
            self.password = password

        self.autocommit = False
        self._xid = None
        self._statement_nums = set()

        self._caches = {}

        if unix_sock is None and host is not None:
            try:
                self._usock = socket.create_connection(
                    (host, port), timeout, source_address)
            except socket.error as e:
                raise InterfaceError(
                    "Can't create a connection to host " + str(host) +
                    " and port " + str(port) + " (timeout is " + str(timeout) +
                    " and source_address is " +
                    str(source_address) + ").") from e

        elif unix_sock is not None:
            try:
                if not hasattr(socket, "AF_UNIX"):
                    raise InterfaceError(
                        "attempt to connect to unix socket on unsupported "
                        "platform")
                self._usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._usock.settimeout(timeout)
                self._usock.connect(unix_sock)
            except socket.error as e:
                if self._usock is not None:
                    self._usock.close()
                raise InterfaceError("communication error") from e

        else:
            raise ProgrammingError("one of host or unix_sock must be provided")

        if tcp_keepalive:
            self._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if ssl_context is not None:
            try:
                import ssl

                if ssl_context is True:
                    ssl_context = ssl.create_default_context()

                # Int32(8) - Message length, including self.
                # Int32(80877103) - The SSL request code.
                self._usock.sendall(ii_pack(8, 80877103))
                resp = self._usock.recv(1)
                if resp != b'S':
                    raise InterfaceError("Server refuses SSL")

                self._usock = ssl_context.wrap_socket(
                    self._usock, server_hostname=host)
            except ImportError:
                raise InterfaceError(
                    "SSL required but ssl module not available in "
                    "this python installation")

        self._sock = self._usock.makefile(mode="rwb")

        self._flush = self._sock.flush
        self._read = self._sock.read
        self._write = self._sock.write
        self._backend_key_data = None

        self.pg_types = defaultdict(
            lambda: converters.text_in, {
                16: converters.bool_in,  # boolean
                17: converters.bytea_in,  # bytea
                19: converters.text_in,  # name type
                20: int,  # int8
                21: int,  # int2
                22: converters.vector_in,  # int2vector
                23: int,  # int4
                25: converters.text_in,  # TEXT type
                26: int,  # oid
                28: int,  # xid
                114: converters.json_in,  # json
                700: float,  # float4
                701: float,  # float8
                705: converters.text_in,  # unknown
                829: converters.text_in,  # MACADDR type
                869: converters.inet_in,  # inet
                1000: converters.array_bool_in,  # BOOL[]
                1003: converters.array_text_in,  # NAME[]
                1005: converters.array_int_in,  # INT2[]
                1007: converters.array_int_in,  # INT4[]
                1009: converters.array_text_in,  # TEXT[]
                1014: converters.array_text_in,  # CHAR[]
                1015: converters.array_text_in,  # VARCHAR[]
                1016: converters.array_int_in,  # INT8[]
                1021: converters.array_float_in,  # FLOAT4[]
                1022: converters.array_float_in,  # FLOAT8[]
                1042: converters.text_in,  # CHAR type
                1043: converters.text_in,  # VARCHAR type
                1082: converters.date_in,  # date
                1083: converters.time_in,
                1114: converters.timestamp_in,
                1184: converters.timestamptz_in,  # timestamp w/ tz
                1186: converters.timedelta_in,
                1231: converters.array_numeric_in,  # NUMERIC[]
                1263: converters.array_text_in,  # cstring[]
                1700: converters.numeric_in,  # NUMERIC
                2275: converters.text_in,  # cstring
                2950: converters.uuid_in,  # uuid
                3802: converters.json_in,  # jsonb
            }
        )

        self.py_types = {
            type(None): (-1, converters.null_out),  # null
            bool: (16, converters.bool_out),
            bytearray: (17, converters.bytes_out),  # bytea
            20: (20, converters.int_out),  # int8
            21: (21, converters.int_out),  # int2
            23: (23, converters.int_out),  # int4
            converters.PGText: (25, converters.text_out),  # text
            float: (701, converters.float_out),  # float8
            converters.PGEnum: (705, converters.enum_out),
            date: (1082, converters.date_out),  # date
            time: (1083, converters.time_out),  # time
            1114: (1114, converters.timestamp_out),  # timestamp
            converters.PGVarchar: converters.text_out,  # varchar
            1184: (1184, converters.timestamptz_out),  # timestamptz
            converters.PGJson: (114, converters.text_out),
            converters.PGJsonb: (3802, converters.text_out),
            Timedelta: (1186, converters.timedelta_out),
            converters.PGInterval: (1186, converters.pginterval_out),
            Decimal: (1700, converters.numeric_out),  # Decimal
            converters.PGTsvector: (3614, converters.text_out),
            UUID: (2950, converters.uuid_out),  # uuid
            bytes: (17, converters.bytes_out),  # bytea
            str: (705, converters.text_out),  # unknown
            enum.Enum: (705, converters.enum_out),
            IPv4Address: (869, converters.inet_out),  # inet
            IPv6Address: (869, converters.inet_out),  # inet
            IPv4Network: (869, converters.inet_out),  # inet
            IPv6Network: (869, converters.inet_out),  # inet
        }

        self.inspect_funcs = {
            Datetime: self.inspect_datetime,
            list: self.array_inspect,
            tuple: self.array_inspect,
            int: self.inspect_int
        }

        self.message_types = {
            NOTICE_RESPONSE: self.handle_NOTICE_RESPONSE,
            AUTHENTICATION_REQUEST: self.handle_AUTHENTICATION_REQUEST,
            PARAMETER_STATUS: self.handle_PARAMETER_STATUS,
            BACKEND_KEY_DATA: self.handle_BACKEND_KEY_DATA,
            READY_FOR_QUERY: self.handle_READY_FOR_QUERY,
            ROW_DESCRIPTION: self.handle_ROW_DESCRIPTION,
            ERROR_RESPONSE: self.handle_ERROR_RESPONSE,
            EMPTY_QUERY_RESPONSE: self.handle_EMPTY_QUERY_RESPONSE,
            DATA_ROW: self.handle_DATA_ROW,
            COMMAND_COMPLETE: self.handle_COMMAND_COMPLETE,
            PARSE_COMPLETE: self.handle_PARSE_COMPLETE,
            BIND_COMPLETE: self.handle_BIND_COMPLETE,
            CLOSE_COMPLETE: self.handle_CLOSE_COMPLETE,
            PORTAL_SUSPENDED: self.handle_PORTAL_SUSPENDED,
            NO_DATA: self.handle_NO_DATA,
            PARAMETER_DESCRIPTION: self.handle_PARAMETER_DESCRIPTION,
            NOTIFICATION_RESPONSE: self.handle_NOTIFICATION_RESPONSE,
            COPY_DONE: self.handle_COPY_DONE,
            COPY_DATA: self.handle_COPY_DATA,
            COPY_IN_RESPONSE: self.handle_COPY_IN_RESPONSE,
            COPY_OUT_RESPONSE: self.handle_COPY_OUT_RESPONSE}

        # Int32 - Message length, including self.
        # Int32(196608) - Protocol version number.  Version 3.0.
        # Any number of key/value pairs, terminated by a zero byte:
        #   String - A parameter name (user, database, or options)
        #   String - Parameter value
        protocol = 196608
        val = bytearray(i_pack(protocol))

        for k, v in init_params.items():
            val.extend(k.encode('ascii') + NULL_BYTE + v + NULL_BYTE)
        val.append(0)
        self._write(i_pack(len(val) + 4))
        self._write(val)
        self._flush()

        self._cursor = self.cursor()
        code = self.error = None
        while code not in (READY_FOR_QUERY, ERROR_RESPONSE):
            code, data_len = ci_unpack(self._read(5))
            self.message_types[code](self._read(data_len - 4), None)
        if self.error is not None:
            raise self.error

        self.in_transaction = False

    def register_out_adapter(self, typ, oid, out_func):
        self.py_types[typ] = (oid, out_func)

    def register_in_adapter(self, oid, in_func):
        self.pg_types[oid] = in_func

    def handle_ERROR_RESPONSE(self, data, ps):
        msg = dict(
            (
                s[:1].decode(self._client_encoding),
                s[1:].decode(self._client_encoding)) for s in
            data.split(NULL_BYTE) if s != b'')

        response_code = msg[RESPONSE_CODE]
        if response_code == '28000':
            cls = InterfaceError
        elif response_code == '23505':
            cls = IntegrityError
        else:
            cls = ProgrammingError

        self.error = cls(msg)

    def handle_EMPTY_QUERY_RESPONSE(self, data, ps):
        self.error = ProgrammingError("query was empty")

    def handle_CLOSE_COMPLETE(self, data, ps):
        pass

    def handle_PARSE_COMPLETE(self, data, ps):
        # Byte1('1') - Identifier.
        # Int32(4) - Message length, including self.
        pass

    def handle_BIND_COMPLETE(self, data, ps):
        pass

    def handle_PORTAL_SUSPENDED(self, data, cursor):
        pass

    def handle_PARAMETER_DESCRIPTION(self, data, cursor):
        '''
        ParameterDescription (B)

            Byte1('t')

                Identifies the message as a parameter description.
            Int32

                Length of message contents in bytes, including self.
            Int16

                The number of parameters used by the statement (can be zero).

            Then, for each parameter, there is the following:

            Int32

                Specifies the object ID of the parameter data type.
        '''

        # count = h_unpack(data)[0]
        # cursor.parameter_oids = unpack_from("!" + "i" * count, data, 2)

    def handle_COPY_DONE(self, data, ps):
        self._copy_done = True

    def handle_COPY_OUT_RESPONSE(self, data, ps):
        # Int8(1) - 0 textual, 1 binary
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)

        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError(
                "An output stream is required for the COPY OUT response.")

    def handle_COPY_DATA(self, data, ps):
        ps.stream.write(data)

    def handle_COPY_IN_RESPONSE(self, data, ps):
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)
        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError(
                "An input stream is required for the COPY IN response.")

        bffr = bytearray(8192)
        while True:
            bytes_read = ps.stream.readinto(bffr)
            if bytes_read == 0:
                break
            self._write(COPY_DATA + i_pack(bytes_read + 4))
            self._write(bffr[:bytes_read])
            self._flush()

        # Send CopyDone
        # Byte1('c') - Identifier.
        # Int32(4) - Message length, including self.
        self._write(COPY_DONE_MSG)
        self._write(SYNC_MSG)
        self._flush()

    def handle_NOTIFICATION_RESPONSE(self, data, ps):
        ##
        # A message sent if this connection receives a NOTIFY that it was
        # LISTENing for.
        # <p>
        # Stability: Added in pg8000 v1.03.  When limited to accessing
        # properties from a notification event dispatch, stability is
        # guaranteed for v1.xx.
        backend_pid = i_unpack(data)[0]
        idx = 4
        null = data.find(NULL_BYTE, idx) - idx
        condition = data[idx:idx + null].decode("ascii")
        idx += null + 1
        null = data.find(NULL_BYTE, idx) - idx
        # additional_info = data[idx:idx + null]

        self.notifications.append((backend_pid, condition))

    def cursor(self):
        """Creates a :class:`Cursor` object bound to this
        connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        return Cursor(self)

    @property
    def description(self):
        return self._run_cursor._getDescription()

    def run(self, sql, stream=None, **params):
        self._run_cursor.execute(sql, params, stream=stream)
        return tuple(self._run_cursor._cached_rows)

    def commit(self):
        """Commits the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self.execute_unnamed(self._cursor, "commit", None)

    def rollback(self):
        """Rolls back the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if not self.in_transaction:
            return
        self.execute_unnamed(self._cursor, "rollback", None)

    def close(self):
        """Closes the database connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            # Byte1('X') - Identifies the message as a terminate message.
            # Int32(4) - Message length, including self.
            self._write(TERMINATE_MSG)
            self._flush()
            self._sock.close()
        except AttributeError:
            raise InterfaceError("connection is closed")
        except ValueError:
            raise InterfaceError("connection is closed")
        except socket.error:
            pass
        finally:
            self._usock.close()
            self._sock = None

    def handle_AUTHENTICATION_REQUEST(self, data, cursor):
        # Int32 -   An authentication code that represents different
        #           authentication messages:
        #               0 = AuthenticationOk
        #               5 = MD5 pwd
        #               2 = Kerberos v5 (not supported by pg8000)
        #               3 = Cleartext pwd
        #               4 = crypt() pwd (not supported by pg8000)
        #               6 = SCM credential (not supported by pg8000)
        #               7 = GSSAPI (not supported by pg8000)
        #               8 = GSSAPI data (not supported by pg8000)
        #               9 = SSPI (not supported by pg8000)
        # Some authentication messages have additional data following the
        # authentication code.  That data is documented in the appropriate
        # class.
        auth_code = i_unpack(data)[0]
        if auth_code == 0:
            pass
        elif auth_code == 3:
            if self.password is None:
                raise InterfaceError(
                    "server requesting password authentication, but no "
                    "password was provided")
            self._send_message(PASSWORD, self.password + NULL_BYTE)
            self._flush()
        elif auth_code == 5:
            ##
            # A message representing the backend requesting an MD5 hashed
            # password response.  The response will be sent as
            # md5(md5(pwd + login) + salt).

            # Additional message data:
            #  Byte4 - Hash salt.
            salt = b"".join(cccc_unpack(data, 4))
            if self.password is None:
                raise InterfaceError(
                    "server requesting MD5 password authentication, but no "
                    "password was provided")
            pwd = b"md5" + md5(
                md5(self.password + self.user).hexdigest().encode("ascii") +
                salt).hexdigest().encode("ascii")
            # Byte1('p') - Identifies the message as a password message.
            # Int32 - Message length including self.
            # String - The password.  Password may be encrypted.
            self._send_message(PASSWORD, pwd + NULL_BYTE)
            self._flush()

        elif auth_code == 10:
            # AuthenticationSASL
            mechanisms = [
                m.decode('ascii') for m in data[4:-1].split(NULL_BYTE)]

            self.auth = ScramClient(
                mechanisms, self.user.decode('utf8'),
                self.password.decode('utf8'))

            init = self.auth.get_client_first().encode('utf8')

            # SASLInitialResponse
            self._write(
                create_message(
                    PASSWORD,
                    b'SCRAM-SHA-256' + NULL_BYTE + i_pack(len(init)) + init))
            self._flush()

        elif auth_code == 11:
            # AuthenticationSASLContinue
            self.auth.set_server_first(data[4:].decode('utf8'))

            # SASLResponse
            msg = self.auth.get_client_final().encode('utf8')
            self._write(create_message(PASSWORD, msg))
            self._flush()

        elif auth_code == 12:
            # AuthenticationSASLFinal
            self.auth.set_server_final(data[4:].decode('utf8'))

        elif auth_code in (2, 4, 6, 7, 8, 9):
            raise InterfaceError(
                "Authentication method " + str(auth_code) +
                " not supported by pg8000.")
        else:
            raise InterfaceError(
                "Authentication method " + str(auth_code) +
                " not recognized by pg8000.")

    def handle_READY_FOR_QUERY(self, data, ps):
        # Byte1 -   Status indicator.
        self.in_transaction = data != IDLE

    def handle_BACKEND_KEY_DATA(self, data, ps):
        self._backend_key_data = data

    def array_inspect(self, value):
        # Check if array has any values. If empty, we can just assume it's an
        # array of strings
        first_element = array_find_first_element(value)
        if first_element is None:
            oid = 25
            # Use binary ARRAY format to avoid having to properly
            # escape text in the array literals
            array_oid = pg_array_types[oid]
        else:
            # supported array output
            typ = type(first_element)

            if typ == bool:
                oid = 16
                array_oid = pg_array_types[oid]

            elif issubclass(typ, int):
                # special int array support -- send as smallest possible array
                # type
                typ = int
                int2_ok, int4_ok, int8_ok = True, True, True
                for v in array_flatten(value):
                    if v is None:
                        continue
                    if min_int2 < v < max_int2:
                        continue
                    int2_ok = False
                    if min_int4 < v < max_int4:
                        continue
                    int4_ok = False
                    if min_int8 < v < max_int8:
                        continue
                    int8_ok = False
                if int2_ok:
                    array_oid = 1005  # INT2[]
                elif int4_ok:
                    array_oid = 1007  # INT4[]
                elif int8_ok:
                    array_oid = 1016  # INT8[]
                else:
                    raise ArrayContentNotSupportedError(
                        "numeric not supported as array contents")
            else:
                try:
                    oid, _ = self.make_param(first_element)

                    # If unknown or string, assume it's a string array
                    if oid in (705, 1043, 25):
                        oid = 25
                        # Use binary ARRAY format to avoid having to properly
                        # escape text in the array literals
                    array_oid = pg_array_types[oid]
                except KeyError:
                    raise ArrayContentNotSupportedError(
                        "oid " + str(oid) + " not supported as array contents")
                except NotSupportedError:
                    raise ArrayContentNotSupportedError(
                        "type " + str(typ) +
                        " not supported as array contents")

        return (array_oid, self.array_out)

    def array_out(self, ar):
        result = []
        for v in ar:

            if isinstance(v, list):
                val = self.array_out(v)

            elif v is None:
                val = 'NULL'

            elif isinstance(v, str):
                val = converters.array_string_escape(v)

            else:
                _, val = self.make_param(v)

            result.append(val)

        return '{' + ','.join(result) + '}'

    def make_param(self, value):
        typ = type(value)
        try:
            oid, func = self.py_types[typ]
        except KeyError:
            try:
                oid, func = self.inspect_funcs[typ](value)
            except KeyError as e:
                oid, func = None, None
                for k, v in self.py_types.items():
                    try:
                        if isinstance(value, k):
                            oid, func = v
                            break
                    except TypeError:
                        pass

                if oid is None:
                    for k, v in self.inspect_funcs.items():
                        try:
                            if isinstance(value, k):
                                oid, func = v(value)
                                break
                        except TypeError:
                            pass
                        except KeyError:
                            pass

                if oid is None:
                    raise NotSupportedError(
                        "type " + str(e) + " not mapped to pg type")

        return oid, func(value)

    def make_params(self, values):
        return [self.make_param(v) for v in values]

    def inspect_datetime(self, value):
        if value.tzinfo is None:
            return self.py_types[1114]  # timestamp
        else:
            return self.py_types[1184]  # send as timestamptz

    def inspect_int(self, value):
        if min_int2 < value < max_int2:
            return self.py_types[21]
        if min_int4 < value < max_int4:
            return self.py_types[23]
        if min_int8 < value < max_int8:
            return self.py_types[20]
        return self.py_types[Decimal]

    def handle_ROW_DESCRIPTION(self, data, cursor):
        count = h_unpack(data)[0]
        idx = 2
        row_desc = []
        for i in range(count):
            name = data[idx:data.find(NULL_BYTE, idx)]
            idx += len(name) + 1
            field = dict(
                zip((
                    "table_oid", "column_attrnum", "type_oid", "type_size",
                    "type_modifier", "format"), ihihih_unpack(data, idx)))
            field['name'] = name
            idx += 18
            row_desc.append(field)
            field['func'] = self.pg_types[field['type_oid']]
        cursor.row_desc = row_desc

    def send_PARSE(self, statement_name_bin, statement, params):

        val = bytearray(statement_name_bin)
        val.extend(statement.encode(self._client_encoding) + NULL_BYTE)
        val.extend(h_pack(len(params)))
        for oid, _ in params:
            val.extend(i_pack(0 if oid == -1 else oid))

        self._send_message(PARSE, val)

    def send_DESCRIBE_STATEMENT(self, statement_name_bin):
        self._send_message(DESCRIBE, STATEMENT + statement_name_bin)

    def execute_unnamed(self, cursor, operation, vals):
        if vals is None:
            vals = ()

        statement, make_args = convert_paramstyle(cursor.paramstyle, operation)

        args = make_args(vals)
        params = self.make_params(args)

        statement_name_bin = NULL_BYTE

        self.send_PARSE(statement_name_bin, statement, params)
        self.send_DESCRIBE_STATEMENT(statement_name_bin)
        self._write(SYNC_MSG)

        try:
            self._flush()
        except AttributeError as e:
            if self._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e

        self.handle_messages(cursor)

        cursor.input_funcs = tuple(f['func'] for f in cursor.row_desc)

        cursor._cached_rows.clear()
        cursor._row_count = -1

        self.send_BIND(statement_name_bin, params)
        self.send_EXECUTE()
        self._write(SYNC_MSG)
        self._flush()
        self.handle_messages(cursor)

    def prepare(self, operation):
        return PreparedStatement(self, operation)

    def prepare_statement(self, operation, params):
        statement, make_args = convert_paramstyle('named', operation)

        for i in count():
            statement_name = '_'.join(("pg8000", "statement", str(i)))
            statement_name_bin = statement_name.encode('ascii') + NULL_BYTE
            if statement_name_bin not in self._statement_nums:
                self._statement_nums.add(statement_name_bin)
                break

        self.send_PARSE(statement_name_bin, statement, params)
        self.send_DESCRIBE_STATEMENT(statement_name_bin)
        self._write(SYNC_MSG)

        try:
            self._flush()
        except AttributeError as e:
            if self._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e

        self.handle_messages(self._run_cursor)

        row_desc = self._run_cursor.row_desc
        input_funcs = tuple(f['func'] for f in row_desc)

        return statement_name_bin, row_desc, input_funcs

    def execute_named(self, statement_name_bin, params, row_desc, input_funcs):
        cursor = self._run_cursor
        cursor.row_desc, cursor.input_funcs = row_desc, input_funcs

        cursor._cached_rows.clear()
        cursor._row_count = -1

        self.send_BIND(statement_name_bin, params)
        self.send_EXECUTE()
        self._write(SYNC_MSG)
        self._flush()
        self.handle_messages(cursor)

    def _send_message(self, code, data):
        try:
            self._write(code)
            self._write(i_pack(len(data) + 4))
            self._write(data)
            self._write(FLUSH_MSG)
        except ValueError as e:
            if str(e) == "write to closed file":
                raise InterfaceError("connection is closed")
            else:
                raise e
        except AttributeError:
            raise InterfaceError("connection is closed")

    def send_BIND(self, statement_name_bin, params):

        # Byte1('B') - Identifies the Bind command.
        # Int32 - Message length, including self.
        # String - Name of the destination portal.
        # String - Name of the source prepared statement.
        # Int16 - Number of parameter format codes.
        # For each parameter format code:
        #   Int16 - The parameter format code.
        # Int16 - Number of parameter values.
        # For each parameter value:
        #   Int32 - The length of the parameter value, in bytes, not
        #           including this length.  -1 indicates a NULL parameter
        #           value, in which no value bytes follow.
        #   Byte[n] - Value of the parameter.
        # Int16 - The number of result-column format codes.
        # For each result-column format code:
        #   Int16 - The format code.
        retval = bytearray(
            NULL_BYTE + statement_name_bin + h_pack(0) + h_pack(len(params)))

        for _, value in params:
            if value is None:
                retval.extend(i_pack(-1))
            else:
                val = value.encode(self._client_encoding)
                retval.extend(i_pack(len(val)))
                retval.extend(val)
        retval.extend(h_pack(0))

        self._send_message(BIND, retval)

    def send_EXECUTE(self):
        # Byte1('E') - Identifies the message as an execute message.
        # Int32 -   Message length, including self.
        # String -  The name of the portal to execute.
        # Int32 -   Maximum number of rows to return, if portal
        #           contains a query # that returns rows.
        #           0 = no limit.
        self._write(EXECUTE_MSG)
        self._write(FLUSH_MSG)

    def handle_NO_DATA(self, msg, cursor):
        cursor.row_desc = []

    def handle_COMMAND_COMPLETE(self, data, cursor):
        values = data[:-1].split(b' ')
        command = values[0]
        if command in self._commands_with_count:
            row_count = int(values[-1])
            if cursor._row_count == -1:
                cursor._row_count = row_count
            else:
                cursor._row_count += row_count

    def handle_DATA_ROW(self, data, cursor):
        idx = 2
        row = []
        for func in cursor.input_funcs:
            vlen = i_unpack(data, idx)[0]
            idx += 4
            if vlen == -1:
                v = None
            else:
                v = func(
                    str(data[idx:idx + vlen], encoding=self._client_encoding))
                idx += vlen
            row.append(v)
        cursor._cached_rows.append(row)

    def handle_messages(self, cursor):
        code = self.error = None

        while code != READY_FOR_QUERY:
            code, data_len = ci_unpack(self._read(5))
            self.message_types[code](self._read(data_len - 4), cursor)

        if self.error is not None:
            raise self.error

    # Byte1('C') - Identifies the message as a close command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def close_prepared_statement(self, statement_name_bin):
        self._send_message(CLOSE, STATEMENT + statement_name_bin)
        self._write(SYNC_MSG)
        self._flush()
        self.handle_messages(self._cursor)
        self._statement_nums.remove(statement_name_bin)

    # Byte1('N') - Identifier
    # Int32 - Message length
    # Any number of these, followed by a zero byte:
    #   Byte1 - code identifying the field type (see responseKeys)
    #   String - field value
    def handle_NOTICE_RESPONSE(self, data, ps):
        self.notices.append(
            dict((s[0:1], s[1:]) for s in data.split(NULL_BYTE)))

    def handle_PARAMETER_STATUS(self, data, ps):
        pos = data.find(NULL_BYTE)
        key, value = data[:pos], data[pos + 1:-1]
        self.parameter_statuses.append((key, value))
        if key == b"client_encoding":
            encoding = value.decode("ascii").lower()
            self._client_encoding = pg_to_py_encodings.get(encoding, encoding)

        elif key == b"integer_datetimes":
            if value == b'on':
                pass

            else:
                pass

        elif key == b"server_version":
            self._server_version = LooseVersion(value.decode('ascii'))
            if self._server_version < LooseVersion('8.2.0'):
                self._commands_with_count = (
                    b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH")
            elif self._server_version < LooseVersion('9.0.0'):
                self._commands_with_count = (
                    b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH",
                    b"COPY")

    def xid(self, format_id, global_transaction_id, branch_qualifier):
        """Create a Transaction IDs (only global_transaction_id is used in pg)
        format_id and branch_qualifier are not used in postgres
        global_transaction_id may be any string identifier supported by
        postgres returns a tuple
        (format_id, global_transaction_id, branch_qualifier)"""
        return (format_id, global_transaction_id, branch_qualifier)

    def tpc_begin(self, xid):
        """Begins a TPC transaction with the given transaction ID xid.

        This method should be called outside of a transaction (i.e. nothing may
        have executed since the last .commit() or .rollback()).

        Furthermore, it is an error to call .commit() or .rollback() within the
        TPC transaction. A ProgrammingError is raised, if the application calls
        .commit() or .rollback() during an active TPC transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._xid = xid
        if self.autocommit:
            self.execute(self._cursor, "begin transaction", None)

    def tpc_prepare(self):
        """Performs the first phase of a transaction started with .tpc_begin().
        A ProgrammingError is be raised if this method is called outside of a
        TPC transaction.

        After calling .tpc_prepare(), no statements can be executed until
        .tpc_commit() or .tpc_rollback() have been called.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        q = "PREPARE TRANSACTION '%s';" % (self._xid[1],)
        self.execute(self._cursor, q, None)

    def tpc_commit(self, xid=None):
        """When called with no arguments, .tpc_commit() commits a TPC
        transaction previously prepared with .tpc_prepare().

        If .tpc_commit() is called prior to .tpc_prepare(), a single phase
        commit is performed. A transaction manager may choose to do this if
        only a single resource is participating in the global transaction.

        When called with a transaction ID xid, the database commits the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError will be raised. This form should be called outside of
        a transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError(
                "Cannot tpc_commit() without a TPC transaction!")

        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                self.execute(
                    self._cursor, "COMMIT PREPARED '%s';" % (xid[1], ),
                    None)
            else:
                # a single-phase commit
                self.commit()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_rollback(self, xid=None):
        """When called with no arguments, .tpc_rollback() rolls back a TPC
        transaction. It may be called before or after .tpc_prepare().

        When called with a transaction ID xid, it rolls back the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError is raised. This form should be called outside of a
        transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError(
                "Cannot tpc_rollback() without a TPC prepared transaction!")

        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                # a two-phase rollback
                self.execute(
                    self._cursor, "ROLLBACK PREPARED '%s';" % (xid[1],),
                    None)
            else:
                # a single-phase rollback
                self.rollback()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_recover(self):
        """Returns a list of pending transaction IDs suitable for use with
        .tpc_commit(xid) or .tpc_rollback(xid).

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            curs = self.cursor()
            curs.execute("select gid FROM pg_prepared_xacts")
            return [self.xid(0, row[0], '') for row in curs]
        finally:
            self.autocommit = previous_autocommit_mode


# pg element oid -> pg array typeoid
pg_array_types = {
    16: 1000,
    25: 1009,    # TEXT[]
    701: 1022,
    1043: 1009,
    1700: 1231,  # NUMERIC[]
}


def array_find_first_element(arr):
    for v in array_flatten(arr):
        if v is not None:
            return v
    return None


def array_flatten(arr):
    for v in arr:
        if isinstance(v, list):
            for v2 in array_flatten(v):
                yield v2
        else:
            yield v


def array_check_dimensions(arr):
    if len(arr) > 0:
        v0 = arr[0]
        if isinstance(v0, list):
            req_len = len(v0)
            req_inner_lengths = array_check_dimensions(v0)
            for v in arr:
                inner_lengths = array_check_dimensions(v)
                if len(v) != req_len or inner_lengths != req_inner_lengths:
                    raise ArrayDimensionsNotConsistentError(
                        "array dimensions not consistent")
            retval = [req_len]
            retval.extend(req_inner_lengths)
            return retval
        else:
            # make sure nothing else at this level is a list
            for v in arr:
                if isinstance(v, list):
                    raise ArrayDimensionsNotConsistentError(
                        "array dimensions not consistent")
    return []


def array_has_null(arr):
    for v in array_flatten(arr):
        if v is None:
            return True
    return False


# PostgreSQL encodings:
#   http://www.postgresql.org/docs/8.3/interactive/multibyte.html
# Python encodings:
#   http://www.python.org/doc/2.4/lib/standard-encodings.html
#
# Commented out encodings don't require a name change between PostgreSQL and
# Python.  If the py side is None, then the encoding isn't supported.
pg_to_py_encodings = {
    # Not supported:
    "mule_internal": None,
    "euc_tw": None,

    # Name fine as-is:
    # "euc_jp",
    # "euc_jis_2004",
    # "euc_kr",
    # "gb18030",
    # "gbk",
    # "johab",
    # "sjis",
    # "shift_jis_2004",
    # "uhc",
    # "utf8",

    # Different name:
    "euc_cn": "gb2312",
    "iso_8859_5": "is8859_5",
    "iso_8859_6": "is8859_6",
    "iso_8859_7": "is8859_7",
    "iso_8859_8": "is8859_8",
    "koi8": "koi8_r",
    "latin1": "iso8859-1",
    "latin2": "iso8859_2",
    "latin3": "iso8859_3",
    "latin4": "iso8859_4",
    "latin5": "iso8859_9",
    "latin6": "iso8859_10",
    "latin7": "iso8859_13",
    "latin8": "iso8859_14",
    "latin9": "iso8859_15",
    "sql_ascii": "ascii",
    "win866": "cp886",
    "win874": "cp874",
    "win1250": "cp1250",
    "win1251": "cp1251",
    "win1252": "cp1252",
    "win1253": "cp1253",
    "win1254": "cp1254",
    "win1255": "cp1255",
    "win1256": "cp1256",
    "win1257": "cp1257",
    "win1258": "cp1258",
    "unicode": "utf-8",  # Needed for Amazon Redshift
}


class PreparedStatement():
    def __init__(self, con, operation):
        self.con = con
        self.operation = operation
        self.statement, self.make_args = convert_paramstyle('named', operation)
        self.name_map = {}

    def run(self, **vals):

        args = self.make_args(vals)
        params = self.con.make_params(args)
        oids = tuple([i for i, _ in params])
        cursor = self.con._run_cursor

        try:
            name_bin, row_desc, input_funcs = self.name_map[oids]
        except KeyError:
            name_bin, row_desc, input_funcs = self.name_map[oids] = \
                self.con.prepare_statement(self.operation, params)

        try:
            if not self.con.in_transaction and not self.con.autocommit:
                self.con.execute_unnamed(cursor, "begin transaction", None)
            self.con.execute_named(name_bin, params, row_desc, input_funcs)
        except AttributeError as e:
            if self.con is None:
                raise InterfaceError("Cursor closed")
            elif self.con._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e

        return tuple(cursor._cached_rows)

    def close(self):
        for statement_name_bin, _, _ in self.name_map.values():
            self.con.close_prepared_statement(statement_name_bin)
        self.con = None
