:mod:`pg8000.dbapi` --- DBAPI 2.0 PostgreSQL Interface
======================================================

.. module:: pg8000.dbapi
    :synopsis: DBAPI 2.0 compliant PostgreSQL interface using pg8000

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

    .. attribute:: Error
    .. attribute:: Warning
    .. attribute:: InterfaceError
    .. attribute:: DatabaseError
    .. attribute:: InternalError
    .. attribute:: OperationalError
    .. attribute:: ProgrammingError
    .. attribute:: IntegrityError
    .. attribute:: DataError
    .. attribute:: NotSupportedError

        All of the standard database exception types are accessible via
        connection instances.

        This is a DB-API 2.0 extension.  Accessing any of these attributes will
        generate a warning, eg. ``DB-API extension connection.DatabaseError
        used``.


.. class:: CursorWrapper

    To construct an instance of this class, use the
    :func:`pg8000.dbapi.ConnectionWrapper.cursor` method.

    .. attribute:: arraysize

        This read/write attribute specifies the number of rows to fetch at a
        time with :func:`CursorWrapper.fetchmany`.  It defaults to 1.

    .. method:: fetchmany()

        Fetch rows
        


.. attribute:: STRING
.. attribute:: BINARY
.. attribute:: NUMBER
.. attribute:: DATETIME
.. attribute:: ROWID

.. class:: Error
.. class:: Warning
.. class:: InterfaceError
.. class:: DatabaseError
.. class:: InternalError
.. class:: OperationalError
.. class:: ProgrammingError
.. class:: IntegrityError
.. class:: DataError
.. class:: NotSupportedError

