:mod:`pg8000.dbapi` --- DBAPI 2.0 PostgreSQL Interface
======================================================

.. module:: pg8000.dbapi
    :synopsis: DBAPI 2.0 compliant PostgreSQL interface using pg8000

DBAPI Properties
----------------

.. attribute:: apilevel
    
    The DBAPI level supported, currently "2.0".

    This property is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. attribute:: threadsafety

    Integer constant stating the level of thread safety the DBAPI interface
    supports.  This DBAPI module supports sharing the module, connections, and
    cursors, resulting in a threadsafety value of 3.

    This property is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. attribute:: paramstyle

    String property stating the type of parameter marker formatting expected by
    the interface.  This value defaults to "format", in which parameters are
    marked in this format: "WHERE name=%s".

    This property is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    As an extension to the DBAPI specification, this value is not constant; it
    can be changed to any of the following values:

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

.. attribute:: STRING
.. attribute:: BINARY
.. attribute:: NUMBER
.. attribute:: DATETIME
.. attribute:: ROWID


DBAPI Functions
---------------

.. function:: connect(user[, host, unix_sock, port=5432, database, password, socket_timeout=60, ssl=False])
    
    Creates a connection to a PostgreSQL database.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_; however, the arguments of the
    function are not defined by the specification.  pg8000 guarentees that for
    all v1.xx releases, no optional parameters will be removed from the
    function definition.

    :param user:
        The username to connect to the PostgreSQL server with.  This
        parameter is required.

    :keyword host:
        The hostname of the PostgreSQL server to connect with.  Providing this
        parameter is necessary for TCP/IP connections.  One of either ``host``
        or ``unix_sock`` must be provided.

    :keyword unix_sock:
        The path to the UNIX socket to access the database through, for
        example, ``'/tmp/.s.PGSQL.5432'``.  One of either ``host`` or
        ``unix_sock`` must be provided.

    :keyword port:
        The TCP/IP port of the PostgreSQL server instance.  This parameter
        defaults to ``5432``, the registered common port of PostgreSQL TCP/IP
        servers.

    :keyword database:
        The name of the database instance to connect with.  This parameter is
        optional; if omitted, the PostgreSQL server will assume the database
        name is the same as the username.

    :keyword password:
        The user password to connect to the server with.  This parameter is
        optional; if omitted and the database server requests password-based
        authentication, the connection will fail to open.  If this parameter
        is provided but not requested by the server, no error will occur.

    :keyword socket_timeout:
        Socket connect timeout measured in seconds.  This parameter defaults to
        60 seconds.

    :keyword ssl:
        Use SSL encryption for TCP/IP sockets if ``True``.  Defaults to
        ``False``.

    :rtype:
        An instance of :class:`pg8000.dbapi.ConnectionWrapper`.

.. function:: Date(year, month, day)

    Constuct an object holding a date value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`

.. function:: Time(hour, minute, second)

    Construct an object holding a time value.
    
    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`

.. function:: Timestamp(year, month, day, hour, minute, second)

    Construct an object holding a timestamp value.
    
    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`

.. function:: DateFromTicks(ticks)

    Construct an object holding a date value from the given ticks value (number
    of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`

.. function:: TimeFromTicks(ticks)

    Construct an objet holding a time value from the given ticks value (number
    of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`

.. function:: TimestampFromTicks(ticks)

    Construct an object holding a timestamp value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`

.. function:: Binary(string)

    Construct an object holding binary data.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`pg8000.types.Bytea`


DBAPI Objects
-------------

.. class:: ConnectionWrapper

    A ``ConnectionWrapper`` instance represents a single physical connection
    to a PostgreSQL database.  To construct an instance of this class, use the
    :func:`pg8000.dbapi.connect` function.

    .. method:: cursor()

        Creates a :class:`pg8000.dbapi.CursorWrapper` instance bound to this
        connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. method:: commit()
    
        Commits the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. method:: rollback()

        Rolls back the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. method:: close()

        Closes the database connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. attribute:: notifies

        A list of server-side notifications received by this database
        connection (via the LISTEN/NOTIFY PostgreSQL commands).  Each list
        element is a two-element tuple containing the PostgreSQL backend PID
        that issued the notify, and the notification name.

        PostgreSQL will only send notifications to a client between
        transactions.  The contents of this property are generally only
        populated after a commit or rollback of the current transaction.

        This list can be modified by a client application to clean out
        notifications as they are handled.  However, inspecting or modifying
        this collection should only be done while holding the
        :attr:`notifies_lock` lock in order to guarantee thread-safety.

        This attribute is not part of the DBAPI standard; it is a pg8000
        extension.

    .. attribute:: notifies_lock

        A ``threading.Lock`` object that should be held to read or modify the
        contents of the :attr:`notifies` list.

        This attribute is not part of the DBAPI standard; it is a pg8000
        extension.

    .. attribute:: Error
                   Warning
                   InterfaceError
                   DatabaseError
                   InternalError
                   OperationalError
                   ProgrammingError
                   IntegrityError
                   DataError
                   NotSupportedError

        All of the standard database exception types are accessible via
        connection instances.

        This is a DBAPI 2.0 extension.  Accessing any of these attributes will
        generate the warning ``DB-API extension connection.DatabaseError
        used``.


.. class:: CursorWrapper

    To construct an instance of this class, use the
    :func:`pg8000.dbapi.ConnectionWrapper.cursor` method.

    .. attribute:: arraysize

        This read/write attribute specifies the number of rows to fetch at a
        time with :meth:`fetchmany`.  It defaults to 1.

    .. attribute:: connection

        This read-only attribute contains a reference to the connection object
        (an instance of :class:`ConnectionWrapper`) on which the cursor was
        created.

        This attribute is part of a DBAPI 2.0 extension.  Accessing this
        attribute will generate the following warning: ``DB-API extension
        cursor.connection used``.

    .. attribute:: rowcount

        This read-only attribute contains the number of rows that the last
        execute method produced (for query statements like ``SELECT``) or
        affected (for modification statements like ``UPDATE``).

        During a query statement, accessing this property requires reading the
        entire result set into memory.  It is preferable to avoid using this
        attribute to reduce memory usage.

        The value is -1 in case no execute method has been performed on the
        cursor, or there was no rowcount associated with the last operation.

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

    .. method:: execute(operation, args=())

        Executes a database operation.  Parameters may be provided as a
        sequence, or as a mapping, depending upon the value of
        :data:`pg8000.dbapi.paramstyle`.

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

    .. method:: executemany(operation, parameter_sets)
    
        Prepare a database operation, and then execute it against all parameter
        sequences or mappings provided.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute
        :param parameter_sets:
            A sequence of parameters to execute the statement with.  The values in
            the sequence should be sequences or mappings of parameters, the same as
            the args argument of the :meth:`execute` method.

    .. method:: fetchone()

        Fetch the next row of a query result set.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:
            A row as a sequence of field values, or ``None`` if no more rows
            are available.

    .. method:: fetchmany(size=None)

        Fetches the next set of rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param size:
            
            The number of rows to fetch when called.  If not provided, the
            :attr:`arraysize` attribute value is used instead.

        :returns:
        
            A sequence, each entry of which is a sequence of field values
            making up a row.  If no more rows are available, an empty sequence
            will be returned.

    .. method:: fetchall()

        Fetches all remaining rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.

    .. method:: copy_from(fileobj, table, sep='\t', null=None)
                copy_from(fileobj, query=)
                copy_to(fileobj, table, sep='\t', null=None)
                copy_to(fileobj, query=)

        Performs a PostgreSQL COPY query to stream data in or out of the
        PostgreSQL server.

        For the copy_from method, the ``fileobj`` parameter must be a file-like
        object that supports the ``read`` method.  For the copy_to method, the
        object must support the ``write`` method.
        
        If the ``table`` parameter is provided, a text COPY command is
        constructed in the form of ``COPY table (TO/FROM) STDOUT ...`` with the
        supplied seperator and null-text value.

        Alternatively, a fully COPY query can be provided with the query
        keyword argument.  This permits the usage of additional COPY directives
        that may be supported by the server.

        These methods are not part of the standard DBAPI, they are a pg8000
        extension as of pg8000 v1.07.   They are designed to be compatible with
        similar methods provided by psycopg2.

    .. method:: close()

        Closes the cursor.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. method:: next()
    .. method:: __iter__()

        A cursor object is iterable to retrieve the rows from a query.

        This is a DBAPI 2.0 extension.  Accessing these methods will generate a
        warning, ``DB-API extension cursor.next() used`` and ``DB-API extension
        cursor.__iter__() used``.

    .. method:: setinputsizes(sizes)
    .. method:: setoutputsizes(size[,column])
    
        These methods are part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, they are not
        implemented by pg8000.


DBAPI Exceptions
----------------

.. exception:: Warning(exceptions.StandardError)

    See :exc:`pg8000.errors.Warning`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: Error(exceptions.StandardError)

    See :exc:`pg8000.errors.Error`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: InterfaceError(Error)

    See :exc:`pg8000.errors.InterfaceError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: DatabaseError(Error)

    See :exc:`pg8000.errors.DatabaseError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: InternalError(DatabaseError)

    See :exc:`pg8000.errors.InternalError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: OperationalError(DatabaseError)

    See :exc:`pg8000.errors.OperationalError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: ProgrammingError(DatabaseError)

    See :exc:`pg8000.errors.ProgrammingError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: IntegrityError(DatabaseError)

    See :exc:`pg8000.errors.IntegrityError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: DataError(DatabaseError)

    See :exc:`pg8000.errors.DataError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: NotSupportedError(DatabaseError)

    See :exc:`pg8000.errors.NotSupportedError`

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

