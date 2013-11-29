Quick Start
===========

Installation
------------
pg8000 is available for Python 2.5, 2.6, 2.7, 3.2 and 3.3 (and has been tested
on CPython, Jython and PyPy).

To install pg8000 using `pip <https://pypi.python.org/pypi/pip>`_ type:

``pip install pg8000``

Interactive Example
-------------------


.. code-block:: python

    >>> import pg8000

    >>> conn = pg8000.connect(host="pgsqldev", user="jack", password="jack123")

    >>> cursor = conn.cursor()
    >>> cursor.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")

    >>> cursor.execute(
    ...     "INSERT INTO book (title) VALUES (%s), (%s) RETURNING id, title",
    ...     ("Ender's Game", "Speaker for the Dead"))
    >>> for row in cursor:
    ...     id, title = row
    ...     print "id = %s, title = %s" % (id, title)
    id = 1, title = Ender's Game
    id = 2, title = Speaker for the Dead
    >>> conn.commit()

    >>> cursor.execute("SELECT now()")
    >>> cursor.fetchone()
    (datetime.datetime(2008, 12, 10, 20, 39, 44, 111612, tzinfo=<UTC>),)

    >>> import datetime
    >>> cursor.execute("SELECT now() - %s", (datetime.date(1980, 4, 27),))
    >>> cursor.fetchone()
    (<<Interval 0 months 10454 days 49184111612 microseconds>,)

    >>> pg8000.paramstyle = "numeric"
    >>> cursor.execute("SELECT array_prepend(:1, :2)", ( 500, [1, 2, 3, 4], ))
    >>> cursor.fetchone()
    ([500, 1, 2, 3, 4],)

    Following the DB-API specification, autocommit is off by default. It can be
    turned on by using the autocommit property of the connection.

    >>> conn.autocommit = True
    >>> cur = conn.cursor()
    >>> cur.execute("vacuum")
    >>> conn.autocommit = False
    
    >>> cursor.close()
    >>> conn.close()
