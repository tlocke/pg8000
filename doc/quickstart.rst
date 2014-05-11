Quick Start
===========

Installation
------------
pg8000 is available for Python 2.5, 2.6, 2.7, 3.2, 3.3 and 3.4 (and has been
tested on CPython, Jython and PyPy).

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
    >>> conn.close()

Try the use_cache feature:

.. code-block:: python

    >>> conn = pg8000.connect(
    ... user="postgres", password="C.P.Snow", use_cache=True)
    >>> cur = conn.cursor()
    >>> cur.execute("select cast(%s as varchar) as f1", ('Troon',))
    >>> res = cur.fetchall()

Now subsequent queries with the same parameter types and SQL will use the
cached prepared statement.

.. code-block:: python

    >>> cur.execute("select cast(%s as varchar) as f1", ('Trunho',))
    >>> res = cur.fetchall()
