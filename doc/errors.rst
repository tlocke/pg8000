:mod:`pg8000.errors` --- pg8000 errors
======================================

.. module:: pg8000.errors
    :synopsis: pg8000 exception classes

pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions.
Generally, more specific exception types will be raised; these specific
exception types will be derived from the generic exceptions.

Generic Exception Classes
-------------------------

.. exception:: Warning(exceptions.StandardError)

    Generic exception raised for important database warnings like data
    truncations.  This exception is not currently used by pg8000.

.. exception:: Error(exceptions.StandardError)

    Generic exception that is the base exception of all other error exceptions.

.. exception:: InterfaceError(Error)

    Generic exception raised for errors that are related to the database interface
    rather than the database itself.  For example, if the interface attempts
    to use an SSL connection but the server refuses, an InterfaceError will
    be raised.

.. exception:: DatabaseError(Error)

    Generic exception raised for errors that are related to the database.  This
    exception is currently never raised by pg8000.

.. exception:: InternalError(DatabaseError)

    Generic exception raised when the database encounters an internal error.  This is
    currently only raised when unexpected state occurs in the pg8000 interface
    itself, and is typically the result of a interface bug.

.. exception:: OperationalError(DatabaseError)

    Generic exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer.  This exception is
    currently never raised by pg8000.

.. exception:: ProgrammingError(DatabaseError)

    Generic exception raised for programming errors.  For example, this exception is
    raised if more parameter fields are in a query string than there are
    available parameters.

.. exception:: IntegrityError(DatabaseError)

    Generic exception raised when the relational integrity of the database is affected.
    This exception is not currently raised by pg8000.

.. exception:: DataError(DatabaseError)

    Generic exception raised for errors that are due to problems with the processed
    data.  This exception is not currently raised by pg8000.

.. exception:: NotSupportedError(DatabaseError)

    Generic exception raised in case a method or database API was used which is not
    supported by the database.


Specific Exception Classes
--------------------------

.. exception:: ConnectionClosedError(InterfaceError)

    Raised when an attempt to use a connection fails due to the connection
    being closed.

.. exception:: ArrayDataParseError(InternalError)

    An exception that is raised when an internal error occurs trying to decode
    binary array data received from the server.  This shouldn't occur unless
    changes to the binary wire format for arrays occur between PostgreSQL
    releases.

.. exception:: ArrayContentNotSupportedError(NotSupportedError)

    Raised when attempting to transmit an array where the base type is not
    supported for binary data transfer by the interface.

.. exception:: ArrayContentNotHomogenousError(ProgrammingError)

    Raised when attempting to transmit an array that doesn't contain only a
    single type of object.

.. exception:: ArrayContentEmptyError(ProgrammingError)

    Raised when attempting to transmit an empty array.  The type oid of an
    empty array cannot be determined, and so sending them is not permitted.

.. exception:: ArrayDimensionsNotConsistentError(ProgrammingError)

    Raised when attempting to transmit an array that has inconsistent
    multi-dimension sizes.

.. exception:: CopyQueryOrTableRequiredError(ProgrammingError)

    Raised when :meth:`~pg8000.dbapi.CursorWrapper.copy_to` or
    :meth:`~pg8000.dbapi.CursorWrapper.copy_from` are called without specifying
    the ``table`` or ``query`` keyword parameters.

    .. versionadded:: 1.07

.. exception:: CopyQueryWithoutStreamError(ProgrammingError)

    Raised when :meth:`~pg8000.dbapi.CursorWrapper.execute` is used to execute
    a ``COPY ...`` query, rather than
    :meth:`~pg8000.dbapi.CursorWrapper.copy_to` or
    :meth:`~pg8000.dbapi.CursorWrapper.copy_from`.

    .. versionadded:: 1.07

