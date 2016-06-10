Release Notes
=============

Version 1.10.6, 2016-06-10
--------------------------
- Fixed a problem where we weren't handling the password connection parameter
  correctly. Now it's handled in the same way as the 'user' and 'database'
  parameters, ie. if the password is bytes, then pass it straight through to the
  database, if it's a string then encode it with utf8.

- It used to be that if the 'user' parameter to the connection function was
  'None', then pg8000 would try and look at environment variables to find a
  username. Now we just go by the 'user' parameter only, and give an error if
  it's None.


Version 1.10.5, 2016-03-04
--------------------------
- Include LICENCE text and sources for docs in the source distribution (the
  tarball).


Version 1.10.4, 2016-02-27
--------------------------
- Fixed bug where if a str is sent as a query parameter, and then with the same
  cursor an int is sent instead of a string, for the same query, then it fails.

- Under Python 2, a str type is now sent 'as is', ie. as a byte string rather
  than trying to decode and send according to the client encoding. Under Python
  2 it's recommended to send text as unicode() objects.

- Dropped and added support for Python versions. Now pg8000 supports
  Python 2.7+ and Python 3.3+. 

- Dropped and added support for PostgreSQL versions. Now pg8000 supports
  PostgreSQL 9.1+.

- pg8000 uses the 'six' library for making the same code run on both Python 2
  and Python 3. We used to include it as a file in the pg8000 source code. Now
  we have it as a separate dependency that's installed with 'pip install'. The
  reason for doing this is that package maintainers for OS distributions
  prefer unbundled libaries.


Version 1.10.3, 2016-01-07
--------------------------
- Removed testing for PostgreSQL 9.0 as it's not longer supported by the
  PostgreSQL Global Development Group.
- Fixed bug where pg8000 would fail with datetimes if PostgreSQL was compiled
  with the integer_datetimes option set to 'off'. The bug was in the
  timestamp_send_float function.


Version 1.10.2, 2015-03-17
--------------------------
- If there's a socket exception thrown when communicating with the database,
  it is now wrapped in an OperationalError exception, to conform to the DB-API
  spec.

- Previously, pg8000 didn't recognize the EmptyQueryResponse (that the server
  sends back if the SQL query is an empty string) now we raise a
  ProgrammingError exception.

- Added socket timeout option for Python 3.

- If the server returns an error, we used to initialize the ProgramerException
  with just the first three fields of the error. Now we initialize the
  ProgrammerException with all the fields.

- Use relative imports inside package.

- User and database names given as bytes. The user and database parameters of
  the connect() function are now passed directly as bytes to the server. If the
  type of the parameter is unicode, pg8000 converts it to bytes using the uft8
  encoding.

- Added support for JSON and JSONB Postgres types. We take the approach of
  taking serialized JSON (str) as an SQL parameter, but returning results as
  de-serialized JSON (Python objects). See the example in the Quickstart.

- Added CircleCI continuous integration.

- String support in arrays now allow letters like "u", braces and whitespace.


Version 1.10.1, 2014-09-15
--------------------------
- Add support for the Wheel package format.

- Remove option to set a connection timeout. For communicating with the server,
  pg8000 uses a file-like object using socket.makefile() but you can't use this
  if the underlying socket has a timeout.


Version 1.10.0, 2014-08-30
--------------------------
- Remove the old ``pg8000.dbapi`` and ``pg8000.DBAPI`` namespaces. For example,
  now only ``pg8000.connect()`` will work, and ``pg8000.dbapi.connect()``
  won't work any more.

- Parse server version string with LooseVersion. This should solve the problems
  that people have been having when using versions of PostgreSQL such as
  ``9.4beta2``.

- Message if portal suspended in autocommit. Give a proper error message if the
  portal is suspended while in autocommit mode. The error is that the portal is
  closed when the transaction is closed, and so in autocommit mode the portal
  will be immediately closed. The bottom line is, don't use autocommit mode if
  there's a chance of retrieving more rows than the cache holds (currently 100).


Version 1.9.14, 2014-08-02
--------------------------

- Make ``executemany()`` set ``rowcount``. Previously, ``executemany()`` would
  always set ``rowcount`` to -1. Now we set it to a meaningful value if
  possible. If any of the statements have a -1 ``rowcount`` then then the
  ``rowcount`` for the ``executemany()`` is -1, otherwise the ``executemany()``
  ``rowcount`` is the sum of the rowcounts of the individual statements.

- Support for password authentication. pg8000 didn't support plain text
  authentication, now it does.


Version 1.9.13, 2014-07-27
--------------------------

- Reverted to using the string ``connection is closed`` as the message of the
  exception that's thrown if a connection is closed. For a few versions we were
  using a slightly different one with capitalization and punctuation, but we've
  reverted to the original because it's easier for users of the library to
  consume.

- Previously, ``tpc_recover()`` would start a transaction if one was not already
  in progress. Now it won't.


Version 1.9.12, 2014-07-22
--------------------------

- Fixed bug in ``tpc_commit()`` where a single phase commit failed.


Version 1.9.11, 2014-07-20
--------------------------

- Add support for two-phase commit DBAPI extension. Thanks to Mariano Reingart's
  TPC code on the Google Code version:

  https://code.google.com/p/pg8000/source/detail?r=c8609701b348b1812c418e2c7

  on which the code for this commit is based.

- Deprecate ``copy_from()`` and ``copy_to()`` The methods ``copy_from()`` and
  ``copy_to()`` of the ``Cursor`` object are deprecated because it's simpler and
  more flexible to use the ``execute()`` method with a ``fileobj`` parameter.

- Fixed bug in reporting unsupported authentication codes. Thanks to
  https://github.com/hackgnar for reporting this and providing the fix.

- Have a default for the ``user`` paramater of the ``connect()`` function. If
  the ``user`` parameter of the ``connect()`` function isn't provided, look
  first for the ``PGUSER`` then the ``USER`` environment variables. Thanks to
  Alex Gaynor https://github.com/alex for this suggestion.

- Before PostgreSQL 8.2, ``COPY`` didn't give row count. Until PostgreSQL 8.2
  (which includes Amazon Redshift which forked at 8.0) the ``COPY`` command
  didn't return a row count, but pg8000 thought it did. That's fixed now.


Version 1.9.10, 2014-06-08
--------------------------
- Remember prepared statements. Now prepared statements are never closed, and
  pg8000 remembers which ones are on the server, and uses them when a query is
  repeated. This gives an increase in performance, because on subsequent
  queries the prepared statement doesn't need to be created each time.

- For performance reasons, pg8000 never closed portals explicitly, it just
  let the server close them at the end of the transaction. However, this can
  cause memory problems for long running transactions, so now pg800 always
  closes a portal after it's exhausted.

- Fixed bug where unicode arrays failed under Python 2. Thanks to
  https://github.com/jdkx for reporting this.

- A FLUSH message is now sent after every message (except SYNC). This is in
  accordance with the protocol docs, and ensures the server sends back its
  responses straight away.


Version 1.9.9, 2014-05-12
-------------------------
- The PostgreSQL interval type is now mapped to datetime.timedelta where
  possible. Previously the PostgreSQL interval type was always mapped to the
  pg8000.Interval type. However, to support the datetime.timedelta type we
  now use it whenever possible. Unfortunately it's not always possible because
  timedelta doesn't support months. If months are needed then the fall-back
  is the pg8000.Interval type. This approach means we handle timedelta in a
  similar way to other Python PostgreSQL drivers, and it makes pg8000
  compatible with popular ORMs like SQLAlchemy.

* Fixed bug in executemany() where a new prepared statement should be created
  for each variation in the oids of the parameter sets.


Version 1.9.8, 2014-05-05
-------------------------
- We used to ask the server for a description of the statement, and then ask
  for a description of each subsequent portal. We now only ask for a
  description of the statement. This results in a significant performance
  improvement, especially for executemany() calls and when using the
  'use_cache' option of the connect() function.

- Fixed warning in Python 3.4 which was saying that a socket hadn't been
  closed. It seems that closing a socket file doesn't close the underlying
  socket.

- Now should cope with PostgreSQL 8 versions before 8.4. This includes Amazon
  Redshift.

- Added 'unicode' alias for 'utf-8', which is needed for Amazon Redshift.

- Various other bug fixes.


Version 1.9.7, 2014-03-26
-------------------------
- Caching of prepared statements. There's now a 'use_cache' boolean parameter
  for the connect() function, which causes all prepared statements to be cached
  by pg8000, keyed on the SQL query string. This should speed things up
  significantly in most cases.

- Added support for the PostgreSQL inet type. It maps to the Python types
  IPv*Address and IPv*Network.

- Added support for PostgreSQL +/- infinity date and timestamp values. Now the
  Python value datetime.datetime.max maps to the PostgreSQL value 'infinity'
  and datetime.datetime.min maps to '-infinity', and the same for
  datetime.date.

- Added support for the PostgreSQL types int2vector and xid, which are mostly
  used internally by PostgreSQL.


Version 1.9.6, 2014-02-26
-------------------------
- Fixed a bug where 'portal does not exist' errors were being generated. Some
  queries that should have been run in a transaction were run in autocommit
  mode and so any that suspended a portal had the portal immediately closed,
  because a portal can only exist within a transaction. This has been solved by
  determining the transaction status from the READY_FOR_QUERY message.


Version 1.9.5, 2014-02-15
-------------------------
- Removed warn() calls for __next__() and __iter__(). Removing the warn() in
  __next__() improves the performance tests by ~20%.

- Increased performance of timestamp by ~20%. Should also improve timestamptz.

- Moved statement_number and portal_number from module to Connection. This
  should reduce lock contention for cases where there's a single module and
  lots of connections.

- Make decimal_out/in and time_in use client_encoding. These functions used to
  assume ascii, and I can't think of a case where that wouldn't work.
  Nonetheless, that theoretical bug is now fixed.

- Fixed a bug in cursor.executemany(), where a non-None parameter in a sequence
  of parameters, is None in a subsequent sequence of parameters.


Version 1.9.4, 2014-01-18
-------------------------
- Fixed a bug where with Python 2, a parameter with the value Decimal('12.44'),
  (and probably other numbers) isn't sent correctly to PostgreSQL, and so the
  command fails. This has been fixed by sending decimal types as text rather
  than binary. I'd imagine it's slightly faster too.


Version 1.9.3, 2014-01-16
-------------------------
- Fixed bug where there were missing trailing zeros after the decimal point in
  the NUMERIC type. For example, the NUMERIC value 1.0 was returned as 1 (with
  no zero after the decimal point).

  This is fixed this by making pg8000 use the text rather than binary
  representation for the numeric type. This actually doubles the speed of
  numeric queries.


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

