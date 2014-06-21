:mod:`pg8000` API
=================

API Reference for pg8000.

Properties
----------

.. attribute:: pg8000.apilevel
    
    The DBAPI level supported, currently "2.0".

    This property is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. attribute:: pg8000.threadsafety

    Integer constant stating the level of thread safety the DBAPI interface
    supports.  This DBAPI module supports sharing the module, connections, and
    cursors, resulting in a threadsafety value of 3.

    This property is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. attribute:: pg8000.paramstyle

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

.. attribute:: pg8000.STRING
.. attribute:: pg8000.BINARY
.. attribute:: pg8000.NUMBER
.. attribute:: pg8000.DATETIME
.. attribute:: pg8000.ROWID


Functions
---------

.. function:: pg8000.connect(user[, host=localhost, unix_sock, port=5432, database, password, socket_timeout=60, ssl=False])
    
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
        or ``unix_sock`` must be provided. The default is ``localhost``.

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
        A :class:`Connection` object.

.. function:: pg8000.Date(year, month, day)

    Constuct an object holding a date value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`

.. function:: pg8000.Time(hour, minute, second)

    Construct an object holding a time value.
    
    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`

.. function:: pg8000.Timestamp(year, month, day, hour, minute, second)

    Construct an object holding a timestamp value.
    
    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`

.. function:: pg8000.DateFromTicks(ticks)

    Construct an object holding a date value from the given ticks value (number
    of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`

.. function:: pg8000.TimeFromTicks(ticks)

    Construct an objet holding a time value from the given ticks value (number
    of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`

.. function:: pg8000.TimestampFromTicks(ticks)

    Construct an object holding a timestamp value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`

.. function:: pg8000.Binary(string)

    Construct an object holding binary data.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`pg8000.types.Bytea`


Generic Exceptions
------------------
pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions.
Generally, more specific exception types are raised; these specific exception
types are derived from the generic exceptions.

.. exception:: pg8000.Warning(exceptions.StandardError)

    Generic exception raised for important database warnings like data
    truncations.  This exception is not currently used by pg8000.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.Error(exceptions.StandardError)

    Generic exception that is the base exception of all other error exceptions.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.InterfaceError(Error)

    Generic exception raised for errors that are related to the database
    interface rather than the database itself.  For example, if the interface
    attempts to use an SSL connection but the server refuses, an InterfaceError
    will be raised.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.DatabaseError(Error)

    Generic exception raised for errors that are related to the database.  This
    exception is currently never raised by pg8000.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.InternalError(DatabaseError)

    Generic exception raised when the database encounters an internal error.
    This is currently only raised when unexpected state occurs in the pg8000
    interface itself, and is typically the result of a interface bug.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.OperationalError(DatabaseError)

    Generic exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer.  This
    exception is currently never raised by pg8000.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.ProgrammingError(DatabaseError)

    Generic exception raised for programming errors.  For example, this
    exception is raised if more parameter fields are in a query string than
    there are available parameters.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.IntegrityError(DatabaseError)

    Generic exception raised when the relational integrity of the database is
    affected.  This exception is not currently raised by pg8000.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.DataError(DatabaseError)

    Generic exception raised for errors that are due to problems with the
    processed data.  This exception is not currently raised by pg8000.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

.. exception:: pg8000.NotSupportedError(DatabaseError)

    Generic exception raised in case a method or database API was used which is
    not supported by the database.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.


Specific Exceptions
-------------------
    
Exceptions that are subclassed from the standard DB-API 2.0 exceptions above.

.. exception:: pg8000.ArrayContentNotSupportedError(NotSupportedError)

    Raised when attempting to transmit an array where the base type is not
    supported for binary data transfer by the interface.

.. exception:: pg8000.ArrayContentNotHomogenousError(ProgrammingError)

    Raised when attempting to transmit an array that doesn't contain only a
    single type of object.

.. exception:: pg8000.ArrayContentEmptyError(ProgrammingError)

    Raised when attempting to transmit an empty array.  The type oid of an
    empty array cannot be determined, and so sending them is not permitted.

.. exception:: pg8000.ArrayDimensionsNotConsistentError(ProgrammingError)

    Raised when attempting to transmit an array that has inconsistent
    multi-dimension sizes.

.. exception:: pg8000.CopyQueryOrTableRequiredError(ProgrammingError)

    Raised when :meth:`CursorWrapper.copy_to` or
    :meth:`Cursor.copy_from` are called without specifying
    the ``table`` or ``query`` keyword parameters.

    .. versionadded:: 1.07

.. exception:: pg8000.CopyQueryWithoutStreamError(ProgrammingError)

    Raised when :meth:`Cursor.execute` is used to execute
    a ``COPY ...`` query, rather than
    :meth:`Cursor.copy_to` or
    :meth:`Cursor.copy_from`.

    .. versionadded:: 1.07

.. exception:: pg8000.QueryParameterIndexError(ProgrammingError)

    Raised when parameters in queries can't be matched with provided parameter
    values.

    .. versionadded:: 1.07

.. exception:: pg8000.QueryParameterParseError(ProgrammingError)

    A parsing error occurred while trying to parse parameters in a query.

    .. versionadded:: 1.07


Classes
-------

.. class:: Connection

    A connection object is retuned by the :func:`pg8000.connect` function.
    It represents a single physical connection to a PostgreSQL database. It has     the following methods:

    .. method:: cursor()

        Creates a :class:`Cursor` object bound to this
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
        
        .. versionadded:: 1.07

    .. attribute:: notifies_lock

        A :class:`threading.Lock` object that should be held to read or modify
        the contents of the :attr:`notifies` list.

        This attribute is not part of the DBAPI standard; it is a pg8000
        extension.

        .. versionadded:: 1.07

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

    .. attribute:: autocommit

    Following the DB-API specification, autocommit is off by default. It can be
    turned on by setting this boolean pg8000-specific autocommit property to
    True.

    .. versionadded:: 1.9

.. class:: Cursor

    A cursor object is returned by the :meth:`~Connection.cursor` method of a connection.
    It has the following attributes and methods:

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
        execute method produced (for query statements like ``SELECT``) or
        affected (for modification statements like ``UPDATE``).

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

        These methods are not part of the standard DBAPI, they are a pg8000
        extension.   They are designed to be compatible with similar methods
        provided by psycopg2.

        :param fileobj:

            A file-like object that data is read from or written to.  For
            copy_from, the object have a ``read`` method; for copy_to, the
            object must have a ``write`` method.

        :param table:

            When the table parameter is provided, a COPY query will be constructed
            in the form of ``COPY table (TO/FROM) STDOUT``.

        :param sep:

            Used only when table is provided, this adds a ``DELIMITER AS``
            clause to the COPY query.

        :param null:
            Used only when table is provided, this adds a ``NULL AS`` clause to
            the COPY query.

        :param query:
            A complete COPY query to be used to generate or insert data.  This
            permits the use of any COPY directives that are supported by the
            server.

        :raises: 

            :exc:`~pg8000.CopyQueryOrTableRequiredError` when neither
            *table* nor *query* parameters are provided.

        .. versionadded:: 1.07

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


Type Classes
------------

.. class:: pg8000.Bytea(str)

    Bytea is a str-derived class that is mapped to a PostgreSQL byte array.
    This class is only used in Python 2, the built-in ``bytes`` type is used in
    Python 3.

.. class:: pg8000.Interval

    An Interval represents a measurement of time.  In PostgreSQL, an interval
    is defined in the measure of months, days, and microseconds; as such, the
    pg8000 interval type represents the same information.

    Note that values of the :attr:`microseconds`, :attr:`days` and
    :attr:`months` properties are independently measured and cannot be
    converted to each other.  A month may be 28, 29, 30, or 31 days, and a day
    may occasionally be lengthened slightly by a leap second.

    .. method:: __init__(self, microseconds, days, months)
    
        Initializes an Interval instance with the given values for
        microseconds, days, and months.

    .. attribute:: microseconds

        Measure of microseconds in the interval.

        The microseconds value is constrained to fit into a signed 64-bit
        integer.  Any attempt to set a value too large or too small will result
        in an OverflowError being raised.

    .. attribute:: days

        Measure of days in the interval.

        The days value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

    .. attribute:: months

        Measure of months in the interval.

        The months value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

