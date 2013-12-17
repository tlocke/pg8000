Release Notes
=============

Version 1.9.2, 2013-12-17
-------------------------
- Fixed incompatibility with PostgreSQL 8.4. In 8.4, the CommandComplete
  message doesn't return a row count if the command is SELECT. We now look at
  the server version and don't look for a row count for a SELECT with version
  8.4.


Version 1.9.1, 2013-12-15
-------------------------
- Fixed bug where the Python 2 'unicode' type wasn't recognized in a query
  parameter.


Version 1.9.0, 2013-12-01
-------------------------
- For Python 3, the :class:`bytes` type replaces the :class:`pg8000.Bytea`
  type. For backward compatibility the :class:`pg8000.Bytea` still works under
  Python 3, but its use is deprecated.

- A single codebase for Python 2 and 3.

- Everything (functions, properties, classes) is now available under the
  ``pg8000`` namespace. So for example:

  - pg8000.DBAPI.connect() -> pg8000.connect()
  - pg8000.DBAPI.apilevel -> pg8000.apilevel
  - pg8000.DBAPI.threadsafety -> pg8000.threadsafety
  - pg8000.DBAPI.paramstyle -> pg8000.paramstyle
  - pg8000.types.Bytea -> pg8000.Bytea
  - pg8000.types.Interval -> pg8000.Interval
  - pg8000.errors.Warning -> pg8000.Warning
  - pg8000.errors.Error -> pg8000.Error
  - pg8000.errors.InterfaceError -> pg8000.InterfaceError
  - pg8000.errors.DatabaseError -> pg8000.DatabaseError

  The old locations are deprecated, but still work for backward compatibility.

- Lots of performance improvements.

  - Faster receiving of ``numeric`` types.
  - Query only parsed when PreparedStatement is created.
  - PreparedStatement re-used in executemany()
  - Use ``collections.deque`` rather than ``list`` for the row cache. We're
    adding to one end and removing from the other. This is O(n) for a list but
    O(1) for a deque.
  - Find the conversion function and do the format code check in the
    ROW_DESCRIPTION handler, rather than every time in the ROW_DATA handler.
  - Use the 'unpack_from' form of struct, when unpacking the data row, so we
    don't have to slice the data.
  - Return row as a list for better performance. At the moment result rows are
    turned into a tuple before being returned. Returning the rows directly as a
    list speeds up the performance tests about 5%.
  - Simplify the event loop. Now the main event loop just continues until a
    READY_FOR_QUERY message is received. This follows the suggestion in the
    Postgres protocol docs. There's not much of a difference in speed, but the
    code is a bit simpler, and it should make things more robust.
  - Re-arrange the code as a state machine to give > 30% speedup.
  - Using pre-compiled struct objects. Pre-compiled struct objects are a bit
    faster than using the struct functions directly. It also hopefully adds to
    the readability of the code.
  - Speeded up _send. Before calling the socket 'write' method, we were
    checking that the 'data' type implements the 'buffer' interface (bytes or
    bytearray), but the check isn't needed because 'write' raises an exception
    if data is of the wrong type.


- Add facility for turning auto-commit on. This follows the suggestion of
  funkybob to fix the problem of not be able to execute a command such as
  'create database' that must be executed outside a transaction. Now you can do
  conn.autocommit = True and then execute 'create database'.

- Add support for the PostgreSQL ``uid`` type. Thanks to Rad Cirskis.

- Add support for the PostgreSQL XML type.

- Add support for the PostgreSQL ``enum`` user defined types.

- Fix a socket leak, where a problem opening a connection could leave a socket
  open.

- Fix empty array issue. https://github.com/mfenniak/pg8000/issues/10

- Fix scale on ``numeric`` types. https://github.com/mfenniak/pg8000/pull/13

- Fix numeric_send. Thanks to Christian Hofstaedtler.


Version 1.08, 2010-06-08
------------------------

- Removed usage of deprecated :mod:`md5` module, replaced with :mod:`hashlib`.
  Thanks to Gavin Sherry for the patch.

- Start transactions on execute or executemany, rather than immediately at the
  end of previous transaction.  Thanks to Ben Moran for the patch.

- Add encoding lookups where needed, to address usage of SQL_ASCII encoding.
  Thanks to Benjamin Schweizer for the patch.

- Remove record type cache SQL query on every new pg8000 connection.

- Fix and test SSL connections.

- Handle out-of-band messages during authentication.


Version 1.07, 2009-01-06
------------------------

- Added support for :meth:`~pg8000.dbapi.CursorWrapper.copy_to` and
  :meth:`~pg8000.dbapi.CursorWrapper.copy_from` methods on cursor objects, to
  allow the usage of the PostgreSQL COPY queries.  Thanks to Bob Ippolito for
  the original patch.

- Added the :attr:`~pg8000.dbapi.ConnectionWrapper.notifies` and
  :attr:`~pg8000.dbapi.ConnectionWrapper.notifies_lock` attributes to DBAPI
  connection objects to provide access to server-side event notifications.
  Thanks again to Bob Ippolito for the original patch.

- Improved performance using buffered socket I/O.

- Added valid range checks for :class:`~pg8000.types.Interval` attributes.

- Added binary transmission of :class:`~decimal.Decimal` values.  This permits
  full support for NUMERIC[] types, both send and receive.

- New `Sphinx <http://sphinx.pocoo.org/>`_-based website and documentation.


Version 1.06, 2008-12-09
------------------------

- pg8000-py3: a branch of pg8000 fully supporting Python 3.0.

- New Sphinx-based documentation.

- Support for PostgreSQL array types -- INT2[], INT4[], INT8[], FLOAT[],
  DOUBLE[], BOOL[], and TEXT[].  New support permits both sending and
  receiving these values.

- Limited support for receiving RECORD types.  If a record type is received,
  it will be translated into a Python dict object.

- Fixed potential threading bug where the socket lock could be lost during 
  error handling.


Version 1.05, 2008-09-03
------------------------

- Proper support for timestamptz field type:

  - Reading a timestamptz field results in a datetime.datetime instance that
    has a valid tzinfo property.  tzinfo is always UTC.

  - Sending a datetime.datetime instance with a tzinfo value will be
    sent as a timestamptz type, with the appropriate tz conversions done.

- Map postgres < -- > python text encodings correctly.

- Fix bug where underscores were not permitted in pyformat names.

- Support "%s" in a pyformat strin.

- Add cursor.connection DB-API extension.

- Add cursor.next and cursor.__iter__ DB-API extensions.

- DBAPI documentation improvements.

- Don't attempt rollback in cursor.execute if a ConnectionClosedError occurs.

- Add warning for accessing exceptions as attributes on the connection object,
  as per DB-API spec.

- Fix up open connection when an unexpected connection occurs, rather than
  leaving the connection in an unusable state.

- Use setuptools/egg package format.


Version 1.04, 2008-05-12
------------------------

- DBAPI 2.0 compatibility:

  - rowcount returns rows affected when appropriate (eg. UPDATE, DELETE)

  - Fix CursorWrapper.description to return a 7 element tuple, as per spec.

  - Fix CursorWrapper.rowcount when using executemany.

  - Fix CursorWrapper.fetchmany to return an empty sequence when no more
    results are available.

  - Add access to DBAPI exceptions through connection properties.

  - Raise exception on closing a closed connection.

  - Change DBAPI.STRING to varchar type.

  - rowcount returns -1 when appropriate.

  - DBAPI implementation now passes Stuart Bishop's Python DB API 2.0 Anal
    Compliance Unit Test.

- Make interface.Cursor class use unnamed prepared statement that binds to
  parameter value types.  This change increases the accuracy of PG's query
  plans by including parameter information, hence increasing performance in
  some scenarios.

- Raise exception when reading from a cursor without a result set.

- Fix bug where a parse error may have rendered a connection unusable.


Version 1.03, 2008-05-09
------------------------

- Separate pg8000.py into multiple python modules within the pg8000 package.
  There should be no need for a client to change how pg8000 is imported.

- Fix bug in row_description property when query has not been completed.

- Fix bug in fetchmany dbapi method that did not properly deal with the end of
  result sets.

- Add close methods to DB connections.

- Add callback event handlers for server notices, notifications, and runtime
  configuration changes.

- Add boolean type output.

- Add date, time, and timestamp types in/out.

- Add recognition of "SQL_ASCII" client encoding, which maps to Python's
  "ascii" encoding.

- Add types.Interval class to represent PostgreSQL's interval data type, and
  appropriate wire send/receive methods.

- Remove unused type conversion methods.


Version 1.02, 2007-03-13
------------------------

- Add complete DB-API 2.0 interface.

- Add basic SSL support via ssl connect bool.

- Rewrite pg8000_test.py to use Python's unittest library.

- Add bytea type support.

- Add support for parameter output types: NULL value, timestamp value, python
  long value.

- Add support for input parameter type oid.


Version 1.01, 2007-03-09
------------------------

- Add support for writing floats and decimal objs up to PG backend.

- Add new error handling code and tests to make sure connection can recover
  from a database error.

- Fixed bug where timestamp types were not always returned in the same binary
  format from the PG backend.  Text format is now being used to send
  timestamps.

- Fixed bug where large packets from the server were not being read fully, due
  to socket.read not always returning full read size requested.  It was a
  lazy-coding bug.

- Added locks to make most of the library thread-safe.

- Added UNIX socket support.


Version 1.00, 2007-03-08
------------------------

- First public release.  Although fully functional, this release is mostly
  lacking in production testing and in type support.

