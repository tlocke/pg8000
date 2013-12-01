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

    >>> cursor.execute("SELECT extract(millennium from now())")
    >>> cursor.fetchone()
    [3.0]

    >>> import datetime
    >>> cursor.execute("SELECT timestamp '2013-12-01 16:06' - %s",
    ... (datetime.date(1980, 4, 27),))
    >>> cursor.fetchone()
    [<Interval 0 months 12271 days 57960000000 microseconds>]

    >>> pg8000.paramstyle = "numeric"
    >>> cursor.execute("SELECT array_prepend(:1, :2)", ( 500, [1, 2, 3, 4], ))
    >>> cursor.fetchone()
    [[500, 1, 2, 3, 4]]
    >>> conn.rollback()

    Following the DB-API specification, autocommit is off by default. It can be
    turned on by using the autocommit property of the connection.

    >>> conn.autocommit = True
    >>> cur = conn.cursor()
    >>> cur.execute("vacuum")
    >>> conn.autocommit = False
    
    >>> cursor.close()
    >>> conn.close()
