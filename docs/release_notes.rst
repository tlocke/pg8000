Release Notes
-------------


Version 1.30.4, 2024-01-03
``````````````````````````

- Add support for more range and multirange types.

- Make the ``Connection.parameter_statuses`` property a ``dict`` rather than a ``dequeue``.


Version 1.30.3, 2023-10-31
``````````````````````````

- Fix problem with PG date overflowing Python types. Now we return the ``str`` we got from the
  server if we can't parse it. 


Version 1.30.2, 2023-09-17
``````````````````````````

- Bug fix where dollar-quoted string constants weren't supported.


Version 1.30.1, 2023-07-29
``````````````````````````

- There was a problem uploading the previous version (1.30.0) to PyPI because the
  markup of the README.rst was invalid. There's now a step in the automated tests to
  check for this.


Version 1.30.0, 2023-07-27
``````````````````````````

- Remove support for Python 3.7

- Add a ``sock`` keyword parameter for creating a connection from a pre-configured
  socket.


Version 1.29.8, 2023-06-16
``````````````````````````

- Ranges don't work with legacy API.


Version 1.29.7, 2023-06-16
``````````````````````````

- Add support for PostgreSQL ``range`` and ``multirange`` types. Previously pg8000
  would just return them as strings, but now they're returned as ``Range`` and lists of
  ``Range``.

- The PostgreSQL ``record`` type is now returned as a ``tuple`` of strings, whereas
  before it was returned as one string.


Version 1.29.6, 2023-05-29
``````````````````````````

- Fixed two bugs with composite types. Nulls should be represented by an empty string,
  and in an array of composite types, the elements should be surrounded by double
  quotes.


Version 1.29.5, 2023-05-09
``````````````````````````

- Fixed bug where pg8000 didn't handle the case when the number of bytes received from
  a socket was fewer than requested. This was being interpreted as a network error, but
  in fact we just needed to wait until more bytes were available.

- When using the ``PGInterval`` type, if a response from the server contained the period
  ``millennium``, it wasn't recognised. This was caused by a spelling mistake where we
  had ``millenium`` rather than ``millennium``.

- Added support for sending PostgreSQL composite types. If a value is sent as a
  ``tuple``, pg8000 will send it to the server as a ``(`` delimited composite string.


Version 1.29.4, 2022-12-14
``````````````````````````

- Fixed bug in ``pg8000.dbapi`` in the ``setinputsizes()`` method where if a ``size``
  was a recognized Python type, the method failed.


Version 1.29.3, 2022-10-26
``````````````````````````

- Upgrade the SCRAM library to version 1.4.3. This adds support for the case where the
  client supports channel binding but the server doesn't.


Version 1.29.2, 2022-10-09
``````````````````````````

- Fixed a bug where in a literal array, items such as ``\n`` and ``\r`` weren't
  escaped properly before being sent to the server.

- Fixed a bug where if the PostgreSQL server has a half-hour time zone set, values of
  type ``timestamp with time zone`` failed. This has been fixed by using the ``parse``
  function of the ``dateutil`` package if the ``datetime`` parser fails.


Version 1.29.1, 2022-05-23
``````````````````````````

- In trying to determine if there's been a failed commit, check for ``ROLLBACK TO
  SAVEPOINT``.


Version 1.29.0, 2022-05-21
``````````````````````````

- Implement a workaround for the `silent failed commit
  <https://github.com/tlocke/pg8000/issues/36>`_ bug.

- Previously if an empty string was sent as the query an exception would be raised, but
  that isn't done now.


Version 1.28.3, 2022-05-18
``````````````````````````

- Put back ``__version__`` attributes that were inadvertently removed.


Version 1.28.2, 2022-05-17
``````````````````````````

- Use a build system that's compliant with PEP517.


Version 1.28.1, 2022-05-17
``````````````````````````

- If when doing a ``COPY FROM`` the ``stream`` parameter is an iterator of ``str``,
  pg8000 used to silently append a newline to the end. That no longer happens.


Version 1.28.0, 2022-05-17
``````````````````````````

- When using the ``COPY FROM`` SQL statement, allow the ``stream`` parameter to be an
  iterable.


Version 1.27.1, 2022-05-16
``````````````````````````

- The ``seconds`` attribute of ``PGInterval`` is now always a ``float``, to cope with
  fractional seconds.

- Updated the ``interval`` parsers for ``iso_8601`` and ``sql_standard`` to take
  account of fractional seconds.


Version 1.27.0, 2022-05-16
``````````````````````````

- It used to be that by default, if pg8000 received an ``interval`` type from the server
  and it was too big to fit into a ``datetime.timedelta`` then an exception would be
  raised. Now if an interval is too big for ``datetime.timedelta`` a ``PGInterval`` is
  returned.

* pg8000 now supports all the output formats for an ``interval`` (``postgres``,
  ``postgres_verbose``, ``iso_8601`` and ``sql_standard``).


Version 1.26.1, 2022-04-23
``````````````````````````

- Make sure all tests are run by the GitHub Actions tests on commit.
- Remove support for Python 3.6
- Remove support for PostgreSQL 9.6


Version 1.26.0, 2022-04-18
``````````````````````````

- When connecting, raise an ``InterfaceError('network error')`` rather than let the
  underlying ``struct.error`` float up.

- Make licence text the same as that used by the OSI. Previously the licence wording
  differed slightly from the BSD 3 Clause licence at
  https://opensource.org/licenses/BSD-3-Clause. This meant that automated tools didn't
  pick it up as being Open Source. The changes are believed to not alter the meaning of   the license at all.


Version 1.25.0, 2022-04-17
``````````````````````````

- Fix more cases where a ``ResourceWarning`` would be raise because of a socket that had
  been left open.

- We now have a single ``InterfaceError`` with the message 'network error' for all
  network errors, with the underlying exception held in the ``cause`` of the exception.


Version 1.24.2, 2022-04-15
``````````````````````````

- To prevent a ``ResourceWarning`` close socket if a connection can't be created.


Version 1.24.1, 2022-03-02
``````````````````````````

- Return pg +/-infinity dates as ``str``. Previously +/-infinity pg values would cause
  an error when returned, but now we return +/-infinity as strings.


Version 1.24.0, 2022-02-06
``````````````````````````

- Add SQL escape functions identifier() and literal() to the native API. For use when a
  query can't be parameterised and the SQL string has to be created using untrusted
  values.


Version 1.23.0, 2021-11-13
``````````````````````````

- If a query has no parameters, then the query will no longer be parsed. Although there
  are performance benefits for doing this, the main reason is to avoid query rewriting,
  which can introduce errors.


Version 1.22.1, 2021-11-10
``````````````````````````

- Fix bug in PGInterval type where ``str()`` failed for a millennia value.


Version 1.22.0, 2021-10-13
``````````````````````````

- Rather than specifying the oids in the ``Parse`` step of the Postgres protocol, pg8000
  now omits them, and so Postgres will use the oids it determines from the query. This
  makes the pg8000 code simpler and also it should also make the nuances of type
  matching more straightforward.
