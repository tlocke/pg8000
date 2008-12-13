Interactive Example
===================


.. code-block:: python

    >>> from pg8000 import DBAPI

    >>> conn = DBAPI.connect(host="pgsqldev4", user="jack", password="jack123")

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

    >>> DBAPI.paramstyle = "numeric"
    >>> cursor.execute("SELECT array_prepend(:1, :2)", ( 500, [1, 2, 3, 4], ))
    >>> cursor.fetchone()
    ([500, 1, 2, 3, 4],)

    >>> cursor.close()
    >>> conn.close()

