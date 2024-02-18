Native API Docs
---------------

pg8000.native.Error
```````````````````

Generic exception that is the base exception of the other error exceptions.


pg8000.native.InterfaceError
````````````````````````````

For errors that originate within pg8000.


pg8000.native.DatabaseError
```````````````````````````

For errors that originate from the server.

pg8000.native.Connection(user, host='localhost', database=None, port=5432, password=None, source_address=None, unix_sock=None, ssl_context=None, timeout=None, tcp_keepalive=True, application_name=None, replication=None, sock=None)
``````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

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
  omitted and the database server requests password-based authentication, the connection
  will fail to open. If this parameter is provided but not
  requested by the server, no error will occur.

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
    |ssl.create_default_context()|_

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
  <https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME>`_.
  If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide values as bytes, eg.  ``'my_application_name'.encode('EUC-JP')``. The default
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

pg8000.native.Connection.notifications
``````````````````````````````````````

A deque of server-side `notifications
<https://www.postgresql.org/docs/current/sql-notify.html>`__ received by this database
connection (via the ``LISTEN`` / ``NOTIFY`` PostgreSQL commands). Each list item is a
three-element tuple containing the PostgreSQL backend PID that issued the notify, the
channel and the payload.


pg8000.native.Connection.notices
````````````````````````````````

A deque of server-side notices received by this database connection.


pg8000.native.Connection.parameter_statuses
```````````````````````````````````````````

A deque of server-side parameter statuses received by this database connection.


pg8000.native.Connection.run(sql, stream=None, types=None, \*\*kwargs)
``````````````````````````````````````````````````````````````````````

Executes an sql statement, and returns the results as a ``list``. For example::

  con.run("SELECT * FROM cities where population > :pop", pop=10000)

sql
  The SQL statement to execute. Parameter placeholders appear as a ``:`` followed by the
  parameter name.

stream
  For use with the PostgreSQL `COPY
  <http://www.postgresql.org/docs/current/static/sql-copy.html>`__ command. The nature
  of the parameter depends on whether the SQL command is ``COPY FROM`` or ``COPY TO``.

  ``COPY FROM``
    The stream parameter must be a readable file-like object or an iterable. If it's an
    iterable then the items can be ``str`` or binary.
  ``COPY TO``
    The stream parameter must be a writable file-like object.

types
  A dictionary of oids. A key corresponds to a parameter. 

kwargs
  The parameters of the SQL statement.


pg8000.native.Connection.row_count
``````````````````````````````````

This read-only attribute contains the number of rows that the last ``run()`` method
produced (for query statements like ``SELECT``) or affected (for modification statements
like ``UPDATE``.

The value is -1 if:

- No ``run()`` method has been performed yet.
- There was no rowcount associated with the last ``run()``.


pg8000.native.Connection.columns
````````````````````````````````

A list of column metadata. Each item in the list is a dictionary with the following
keys:

- name
- table_oid
- column_attrnum
- type_oid
- type_size
- type_modifier
- format


pg8000.native.Connection.close()
````````````````````````````````

Closes the database connection.


pg8000.native.Connection.register_out_adapter(typ, out_func)
````````````````````````````````````````````````````````````

Register a type adapter for types going out from pg8000 to the server.

typ
  The Python class that the adapter is for.

out_func
  A function that takes the Python object and returns its string representation
  in the format that the server requires.


pg8000.native.Connection.register_in_adapter(oid, in_func)
``````````````````````````````````````````````````````````

Register a type adapter for types coming in from the server to pg8000.

oid
  The PostgreSQL type identifier found in the `pg_type system catalog
  <https://www.postgresql.org/docs/current/catalog-pg-type.html>`_.

in_func
  A function that takes the PostgreSQL string representation and returns a corresponding
  Python object.


pg8000.native.Connection.prepare(sql)
`````````````````````````````````````

Returns a ``PreparedStatement`` object which represents a `prepared statement
<https://www.postgresql.org/docs/current/sql-prepare.html>`_ on the server. It can
subsequently be repeatedly executed.

sql
  The SQL statement to prepare. Parameter placeholders appear as a ``:`` followed by the
  parameter name.


pg8000.native.PreparedStatement
```````````````````````````````

A prepared statement object is returned by the ``pg8000.native.Connection.prepare()``
method of a connection. It has the following methods:


pg8000.native.PreparedStatement.run(\*\*kwargs)
```````````````````````````````````````````````

Executes the prepared statement, and returns the results as a ``tuple``.

kwargs
  The parameters of the prepared statement.


pg8000.native.PreparedStatement.close()
```````````````````````````````````````

Closes the prepared statement, releasing the prepared statement held on the server.


pg8000.native.identifier(ident)
```````````````````````````````

Correctly quotes and escapes a string to be used as an `SQL identifier
<https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS>`_.

ident
  The ``str`` to be used as an SQL identifier.


pg8000.native.literal(value)
````````````````````````````

Correctly quotes and escapes a value to be used as an `SQL literal
<https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS>`_.

value
  The value to be used as an SQL literal.
