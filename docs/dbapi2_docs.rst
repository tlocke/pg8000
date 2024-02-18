DB-API 2 Docs
-------------


Properties
``````````


pg8000.dbapi.apilevel
:::::::::::::::::::::

The DBAPI level supported, currently "2.0".


pg8000.dbapi.threadsafety
:::::::::::::::::::::::::

Integer constant stating the level of thread safety the DBAPI interface supports. For
pg8000, the threadsafety value is 1, meaning that threads may share the module but not
connections.


pg8000.dbapi.paramstyle
:::::::::::::::::::::::

String property stating the type of parameter marker formatting expected by
the interface.  This value defaults to "format", in which parameters are
marked in this format: "WHERE name=%s".

As an extension to the DBAPI specification, this value is not constant; it can be
changed to any of the following values:

qmark
  Question mark style, eg. ``WHERE name=?``

numeric
  Numeric positional style, eg. ``WHERE name=:1``

named
  Named style, eg. ``WHERE name=:paramname``

format
  printf format codes, eg. ``WHERE name=%s``

pyformat
  Python format codes, eg. ``WHERE name=%(paramname)s``


pg8000.dbapi.STRING
:::::::::::::::::::

String type oid.

pg8000.dbapi.BINARY
:::::::::::::::::::


pg8000.dbapi.NUMBER
:::::::::::::::::::

Numeric type oid.


pg8000.dbapi.DATETIME
:::::::::::::::::::::

Timestamp type oid


pg8000.dbapi.ROWID
::::::::::::::::::

ROWID type oid


Functions
`````````

pg8000.dbapi.connect(user, host='localhost', database=None, port=5432, password=None, source_address=None, unix_sock=None, ssl_context=None, timeout=None, tcp_keepalive=True, application_name=None, replication=None, sock=None)
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Creates a connection to a PostgreSQL database.

user
  The username to connect to the PostgreSQL server with. If your server character
  encoding is not ``ascii`` or ``utf8``, then you need to provide ``user`` as bytes,
  eg. ``'my_name'.encode('EUC-JP')``.

host
  The hostname of the PostgreSQL server to connect with. Providing this parameter is
  necessary for TCP/IP connections. One of either ``host`` or ``unix_sock`` must be
  provided. The default is ``localhost``.

database
  The name of the database instance to connect with. If ``None`` then the PostgreSQL
  server will assume the database name is the same as the username. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide ``database``
  as bytes, eg. ``'my_db'.encode('EUC-JP')``.

port
  The TCP/IP port of the PostgreSQL server instance.  This parameter defaults to
  ``5432``, the registered common port of PostgreSQL TCP/IP servers.

password
  The user password to connect to the server with. This parameter is optional; if
  omitted and the database server requests password-based authentication, the
  connection will fail to open. If this parameter is provided but not requested by the
  server, no error will occur.

  If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide ``password`` as bytes, eg.  ``'my_password'.encode('EUC-JP')``.

source_address
  The source IP address which initiates the connection to the PostgreSQL server. The
  default is ``None`` which means that the operating system will choose the source
  address.

unix_sock
  The path to the UNIX socket to access the database through, for example,
  ``'/tmp/.s.PGSQL.5432'``. One of either ``host`` or ``unix_sock`` must be provided.

ssl_context
  This governs SSL encryption for TCP/IP sockets. It can have three values:

  - ``None``, meaning no SSL (the default)
  - ``True``, means use SSL with an |ssl.SSLContext|_ created using
    |ssl.create_default_context()|_.

  - An instance of |ssl.SSLContext|_ which will be used to create the SSL connection.

  If your PostgreSQL server is behind an SSL proxy, you can set the pg8000-specific
  attribute ``ssl.SSLContext.request_ssl = False``, which tells pg8000 to use an SSL
  socket, but not to request SSL from the PostgreSQL server. Note that this means you
  can't use SCRAM authentication with channel binding.

timeout
  This is the time in seconds before the connection to the server will time out. The
  default is ``None`` which means no timeout.

tcp_keepalive
  If ``True`` then use `TCP keepalive
  <https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive>`_. The default is ``True``.

application_name
  Sets the `application_name
  <https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME>`_. If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide values as bytes, eg. ``'my_application_name'.encode('EUC-JP')``. The default
  is ``None`` which means that the server will set the application name.

replication
  Used to run in `streaming replication mode
  <https://www.postgresql.org/docs/current/protocol-replication.html>`_. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide values as
  bytes, eg. ``'database'.encode('EUC-JP')``.

sock
  A socket-like object to use for the connection. For example, ``sock`` could be a plain
  ``socket.socket``, or it could represent an SSH tunnel or perhaps an
  ``ssl.SSLSocket`` to an SSL proxy. If an |ssl.SSLContext| is provided, then it will be
  used to attempt to create an SSL socket from the provided socket. 


pg8000.dbapi.Date(year, month, day)

Construct an object holding a date value.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.

Returns: `datetime.date`


pg8000.dbapi.Time(hour, minute, second)
:::::::::::::::::::::::::::::::::::::::

Construct an object holding a time value.

Returns: ``datetime.time``


pg8000.dbapi.Timestamp(year, month, day, hour, minute, second)
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Construct an object holding a timestamp value.

Returns: ``datetime.datetime``


pg8000.dbapi.DateFromTicks(ticks)
:::::::::::::::::::::::::::::::::

Construct an object holding a date value from the given ticks value (number of seconds
since the epoch).

Returns: ``datetime.datetime``


pg8000.dbapi.TimeFromTicks(ticks)
:::::::::::::::::::::::::::::::::

Construct an object holding a time value from the given ticks value (number of seconds
since the epoch).

Returns: ``datetime.time``


pg8000.dbapi.TimestampFromTicks(ticks)
::::::::::::::::::::::::::::::::::::::

Construct an object holding a timestamp value from the given ticks value (number of
seconds since the epoch).

Returns: ``datetime.datetime``


pg8000.dbapi.Binary(value)
::::::::::::::::::::::::::

Construct an object holding binary data.

Returns: ``bytes``.


Generic Exceptions
``````````````````

Pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions. Generally,
more specific exception types are raised; these specific exception types are derived
from the generic exceptions.

pg8000.dbapi.Warning
::::::::::::::::::::

Generic exception raised for important database warnings like data truncations. This
exception is not currently used by pg8000.


pg8000.dbapi.Error
::::::::::::::::::

Generic exception that is the base exception of all other error exceptions.


pg8000.dbapi.InterfaceError
:::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database interface rather
than the database itself. For example, if the interface attempts to use an SSL
connection but the server refuses, an InterfaceError will be raised.


pg8000.dbapi.DatabaseError
::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database. This exception is
currently never raised by pg8000.


pg8000.dbapi.DataError
::::::::::::::::::::::

Generic exception raised for errors that are due to problems with the processed data.
This exception is not currently raised by pg8000.


pg8000.dbapi.OperationalError
:::::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database's operation and not
necessarily under the control of the programmer. This exception is currently never
raised by pg8000.


pg8000.dbapi.IntegrityError
:::::::::::::::::::::::::::

Generic exception raised when the relational integrity of the database is affected. This
exception is not currently raised by pg8000.


pg8000.dbapi.InternalError
::::::::::::::::::::::::::

Generic exception raised when the database encounters an internal error. This is
currently only raised when unexpected state occurs in the pg8000 interface itself, and
is typically the result of a interface bug.


pg8000.dbapi.ProgrammingError
:::::::::::::::::::::::::::::

Generic exception raised for programming errors. For example, this exception is raised
if more parameter fields are in a query string than there are available parameters.


pg8000.dbapi.NotSupportedError
::::::::::::::::::::::::::::::

Generic exception raised in case a method or database API was used which is not
supported by the database.


Classes
```````


pg8000.dbapi.Connection
:::::::::::::::::::::::

A connection object is returned by the ``pg8000.connect()`` function. It represents a
single physical connection to a PostgreSQL database.


pg8000.dbapi.Connection.autocommit
::::::::::::::::::::::::::::::::::

Following the DB-API specification, autocommit is off by default. It can be turned on by
setting this boolean pg8000-specific autocommit property to ``True``.


pg8000.dbapi.Connection.close()
:::::::::::::::::::::::::::::::

Closes the database connection.


pg8000.dbapi.Connection.cursor()
::::::::::::::::::::::::::::::::

Creates a ``pg8000.dbapi.Cursor`` object bound to this connection.


pg8000.dbapi.Connection.rollback()
::::::::::::::::::::::::::::::::::

Rolls back the current database transaction.


pg8000.dbapi.Connection.tpc_begin(xid)
::::::::::::::::::::::::::::::::::::::

Begins a TPC transaction with the given transaction ID xid. This method should be
called outside of a transaction (i.e. nothing may have executed since the last
``commit()``  or ``rollback()``. Furthermore, it is an error to call ``commit()`` or
``rollback()`` within the TPC transaction. A ``ProgrammingError`` is raised, if the
application calls ``commit()`` or ``rollback()`` during an active TPC transaction.


pg8000.dbapi.Connection.tpc_commit(xid=None)
::::::::::::::::::::::::::::::::::::::::::::

When called with no arguments, ``tpc_commit()`` commits a TPC transaction previously
prepared with ``tpc_prepare()``. If ``tpc_commit()`` is called prior to
``tpc_prepare()``, a single phase commit is performed. A transaction manager may choose
to do this if only a single resource is participating in the global transaction.

When called with a transaction ID ``xid``, the database commits the given transaction.
If an invalid transaction ID is provided, a ``ProgrammingError`` will be raised. This
form should be called outside of a transaction, and is intended for use in recovery.

On return, the TPC transaction is ended.


pg8000.dbapi.Connection.tpc_prepare()
:::::::::::::::::::::::::::::::::::::

Performs the first phase of a transaction started with ``.tpc_begin()``. A
``ProgrammingError`` is be raised if this method is called outside of a TPC transaction.

After calling ``tpc_prepare()``, no statements can be executed until ``tpc_commit()`` or
``tpc_rollback()`` have been called.


pg8000.dbapi.Connection.tpc_recover()
:::::::::::::::::::::::::::::::::::::

Returns a list of pending transaction IDs suitable for use with ``tpc_commit(xid)`` or
``tpc_rollback(xid)``.


pg8000.dbapi.Connection.tpc_rollback(xid=None)
::::::::::::::::::::::::::::::::::::::::::::::

When called with no arguments, ``tpc_rollback()`` rolls back a TPC transaction. It may
be called before or after ``tpc_prepare()``.

When called with a transaction ID xid, it rolls back the given transaction. If an
invalid transaction ID is provided, a ``ProgrammingError`` is raised. This form should
be called outside of a transaction, and is intended for use in recovery.

On return, the TPC transaction is ended.


pg8000.dbapi.Connection.xid(format_id, global_transaction_id, branch_qualifier)
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Create a Transaction IDs (only global_transaction_id is used in pg) format_id and
branch_qualifier are not used in postgres global_transaction_id may be any string
identifier supported by postgres returns a tuple (format_id, global_transaction_id,
branch_qualifier)


pg8000.dbapi.Cursor
:::::::::::::::::::

A cursor object is returned by the ``pg8000.dbapi.Connection.cursor()`` method of a
connection. It has the following attributes and methods:

pg8000.dbapi.Cursor.arraysize
'''''''''''''''''''''''''''''

This read/write attribute specifies the number of rows to fetch at a time with
``pg8000.dbapi.Cursor.fetchmany()``.  It defaults to 1.


pg8000.dbapi.Cursor.connection
''''''''''''''''''''''''''''''

This read-only attribute contains a reference to the connection object (an instance of
``pg8000.dbapi.Connection``) on which the cursor was created.


pg8000.dbapi.Cursor.rowcount
''''''''''''''''''''''''''''

This read-only attribute contains the number of rows that the last ``execute()`` or
``executemany()`` method produced (for query statements like ``SELECT``) or affected
(for modification statements like ``UPDATE``.

The value is -1 if:

- No ``execute()`` or ``executemany()`` method has been performed yet on the cursor.

- There was no rowcount associated with the last ``execute()``.

- At least one of the statements executed as part of an ``executemany()`` had no row
  count associated with it.


pg8000.dbapi.Cursor.description
'''''''''''''''''''''''''''''''

This read-only attribute is a sequence of 7-item sequences. Each value contains
information describing one result column. The 7 items returned for each column are
(name, type_code, display_size, internal_size, precision, scale, null_ok). Only the
first two values are provided by the current implementation.


pg8000.dbapi.Cursor.close()
'''''''''''''''''''''''''''

Closes the cursor.


pg8000.dbapi.Cursor.execute(operation, args=None, stream=None)
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Executes a database operation. Parameters may be provided as a sequence, or as a
mapping, depending upon the value of ``pg8000.dbapi.paramstyle``. Returns the cursor,
which may be iterated over.

operation
  The SQL statement to execute.

args
  If ``pg8000.dbapi.paramstyle`` is ``qmark``, ``numeric``, or ``format``, this
  argument should be an array of parameters to bind into the statement. If
  ``pg8000.dbapi.paramstyle`` is ``named``, the argument should be a ``dict`` mapping of
  parameters. If ``pg8000.dbapi.paramstyle`` is ``pyformat``, the argument value may be
  either an array or a mapping.

stream
  This is a pg8000 extension for use with the PostgreSQL `COPY
  <http://www.postgresql.org/docs/current/static/sql-copy.html>`__ command. For a
  ``COPY FROM`` the parameter must be a readable file-like object, and for ``COPY TO``
  it must be writable.


pg8000.dbapi.Cursor.executemany(operation, param_sets)
''''''''''''''''''''''''''''''''''''''''''''''''''''''

Prepare a database operation, and then execute it against all parameter sequences or
mappings provided.

operation
  The SQL statement to execute.

parameter_sets
  A sequence of parameters to execute the statement with. The values in the sequence
  should be sequences or mappings of parameters, the same as the args argument of the
  ``pg8000.dbapi.Cursor.execute()`` method.


pg8000.dbapi.Cursor.callproc(procname, parameters=None)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''

Call a stored database procedure with the given name and optional parameters.


procname
  The name of the procedure to call.

parameters
  A list of parameters.


pg8000.dbapi.Cursor.fetchall()
''''''''''''''''''''''''''''''

Fetches all remaining rows of a query result.

Returns: A sequence, each entry of which is a sequence of field values making up a row.


pg8000.dbapi.Cursor.fetchmany(size=None)
''''''''''''''''''''''''''''''''''''''''

Fetches the next set of rows of a query result.

size
  The number of rows to fetch when called.  If not provided, the
  ``pg8000.dbapi.Cursor.arraysize`` attribute value is used instead.

Returns: A sequence, each entry of which is a sequence of field values making up a row.
If no more rows are available, an empty sequence will be returned.


pg8000.dbapi.Cursor.fetchone()
''''''''''''''''''''''''''''''

Fetch the next row of a query result set.

Returns: A row as a sequence of field values, or ``None`` if no more rows are available.


pg8000.dbapi.Cursor.setinputsizes(\*sizes)
''''''''''''''''''''''''''''''''''''''''''

Used to set the parameter types of the next query. This is useful if it's difficult for
pg8000 to work out the types from the parameters themselves (eg. for parameters of type
None).

sizes
  Positional parameters that are either the Python type of the parameter to be sent, or
  the PostgreSQL oid. Common oids are available as constants such as ``pg8000.STRING``,
  ``pg8000.INTEGER``, ``pg8000.TIME`` etc.


pg8000.dbapi.Cursor.setoutputsize(size, column=None)
''''''''''''''''''''''''''''''''''''''''''''''''''''

Not implemented by pg8000.


pg8000.dbapi.Interval
'''''''''''''''''''''

An Interval represents a measurement of time.  In PostgreSQL, an interval is defined in
the measure of months, days, and microseconds; as such, the pg8000 interval type
represents the same information.

Note that values of the ``pg8000.dbapi.Interval.microseconds``,
``pg8000.dbapi.Interval.days``, and ``pg8000.dbapi.Interval.months`` properties are
independently measured and cannot be converted to each other. A month may be 28, 29, 30,
or 31 days, and a day may occasionally be lengthened slightly by a leap second.


