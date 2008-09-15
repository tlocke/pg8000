:mod:`pg8000.errors` --- pg8000 errors
======================================

.. module:: pg8000.errors
    :synopsis: pg8000 exception classes

pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions.
Generally, more specific exception types will be raised; these specific
exception types will be derived from the generic exceptions.

Generic Exception Classes
-------------------------

.. class:: Warning(exceptions.StandardError)

    Generic exception raised for important database warnings like data
    truncations.  This exception is not currently used by pg8000.

.. class:: Error(exceptions.StandardError)

    Generic exception that is the base class of all other error exceptions.

.. class:: InterfaceError(Error)

    Generic exception raised for errors that are related to the database interface
    rather than the database itself.  For example, if the interface attempts
    to use an SSL connection but the server refuses, an InterfaceError will
    be raised.

.. class:: DatabaseError(Error)

    Generic exception raised for errors that are related to the database.  This
    exception is currently never raised by pg8000.

.. class:: InternalError(DatabaseError)

    Generic exception raised when the database encounters an internal error.  This is
    currently only raised when unexpected state occurs in the pg8000 interface
    itself, and is typically the result of a interface bug.

.. class:: OperationalError(DatabaseError)

    Generic exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer.  This exception is
    currently never raised by pg8000.

.. class:: ProgrammingError(DatabaseError)

    Generic exception raised for programming errors.  For example, this exception is
    raised if more parameter fields are in a query string than there are
    available parameters.

.. class:: IntegrityError(DatabaseError)

    Generic exception raised when the relational integrity of the database is affected.
    This exception is not currently raised by pg8000.

.. class:: DataError(DatabaseError)

    Generic exception raised for errors that are due to problems with the processed
    data.  This exception is not currently raised by pg8000.

.. class:: NotSupportedError(DatabaseError)

    Generic exception raised in case a method or database API was used which is not
    supported by the database.


Specific Exception Classes
--------------------------

.. class:: ConnectionClosedError(InterfaceError)

    Raised when an attempt to use a connection fails due to the connection
    being closed.

.. class:: ArrayDataParseError(InternalError)

    An exception that is raised when an internal error occurs trying to decode
    binary array data received from the server.  This shouldn't occur unless
    changes to the binary wire format for arrays occur between PostgreSQL
    releases.

.. class:: ArrayContentNotSupportedError(NotSupportedError)

    Raised when attempting to transmit an array where the base type is not
    supported for binary data transfer by the interface.

.. class:: ArrayContentNotHomogenousError(ProgrammingError)

    Raised when attempting to transmit an array that doesn't contain only a
    single type of object.

.. class:: ArrayContentEmptyError(ProgrammingError)

    Raised when attempting to transmit an empty array.  The type oid of an
    empty array cannot be determined, and so sending them is not permitted.

.. class:: ArrayDimensionsNotConsistentError(ProgrammingError)

    Raised when attempting to transmit an array that has inconsistent
    multi-dimension sizes.

