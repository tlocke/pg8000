Quick Start
===========

Key Points
----------

- Runs on Python version 2.5, 2.6, 2.7, 3.2, 3.3 and 3.4
- Runs on CPython, Jython and PyPy
- Although it's possible for threads to share cursors and connections, for
  performance reasons it's best to use one thread per connection.
- Internally, all queries use prepared statements. pg8000 remembers that a
  prepared statement has been created, and uses it on subsequent queries.

Installation
------------

To install pg8000 using `pip <https://pypi.python.org/pypi/pip>`_ type:

``pip install pg8000``


Interactive Example
-------------------

Import pg8000, connect to the database, create a table, add some rows and then
query the table:

.. code-block:: python

    >>> import pg8000
    >>> conn = pg8000.connect(user="postgres", password="C.P.Snow")
    >>> cursor = conn.cursor()
    >>> cursor.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
    >>> cursor.execute(
    ...     "INSERT INTO book (title) VALUES (%s), (%s) RETURNING id, title",
    ...     ("Ender's Game", "Speaker for the Dead"))
    >>> results = cursor.fetchall()
    >>> for row in results:
    ...     id, title = row
    ...     print("id = %s, title = %s" % (id, title))
    id = 1, title = Ender's Game
    id = 2, title = Speaker for the Dead
    >>> conn.commit()

Another query, using some PostgreSQL functions:

.. code-block:: python

    >>> cursor.execute("SELECT extract(millennium from now())")
    >>> cursor.fetchone()
    [3.0]

A query that returns the PostgreSQL interval type:

.. code-block:: python

    >>> import datetime
    >>> cursor.execute("SELECT timestamp '2013-12-01 16:06' - %s",
    ... (datetime.date(1980, 4, 27),))
    >>> cursor.fetchone()
    [datetime.timedelta(12271, 57960)]

pg8000 supports all the DB-API parameter styles. Here's an example of using
the 'numeric' parameter style:

.. code-block:: python

    >>> pg8000.paramstyle = "numeric"
    >>> cursor.execute("SELECT array_prepend(:1, :2)", ( 500, [1, 2, 3, 4], ))
    >>> cursor.fetchone()
    [[500, 1, 2, 3, 4]]
    >>> pg8000.paramstyle = "format"
    >>> conn.rollback()

Following the DB-API specification, autocommit is off by default. It can be
turned on by using the autocommit property of the connection.

.. code-block:: python

    >>> conn.autocommit = True
    >>> cur = conn.cursor()
    >>> cur.execute("vacuum")
    >>> conn.autocommit = False
    >>> cursor.close()

When communicating with the server, pg8000 uses the character set that the
server asks it to use (the client encoding). By default the client encoding is
the database's character set (chosen when the database is created), but the
client encoding can be changed in a number of ways (eg. setting
CLIENT_ENCODING in postgresql.conf). Another way of changing the client
encoding is by using an SQL command. For example:

.. code-block:: python

    >>> cur = conn.cursor()
    >>> cur.execute("SET CLIENT_ENCODING TO 'UTF8'")
    >>> cur.execute("SHOW CLIENT_ENCODING")
    >>> cur.fetchone()
    ['UTF8']
    >>> cur.close()

JSON is sent to the server serialized, and returned de-serialized. Here's an
example:

.. code-block:: python

    >>> import json
    >>> cur = conn.cursor()
    >>> val = ['Apollo 11 Cave', True, 26.003]
    >>> cur.execute("SELECT cast(%s as json)", (json.dumps(val),))
    >>> cur.fetchone()
    [['Apollo 11 Cave', True, 26.003]]
    >>> cur.close()
    >>> conn.close()
