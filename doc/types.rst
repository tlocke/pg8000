Type Mapping
============

The following table shows the mapping between Python types and PostgreSQL
types, and vice versa.

If pg8000 doesn't recognize a type that it receives from PostgreSQL, it will
return it as a ``str`` type. This is how pg8000 handles PostgreSQL ``enum`` and
XML types.

+--------------------------------+-----------------+---------------------------+
| Python Type                    | PostgreSQL Type | Notes                     |
+================================+=================+===========================+
| :class:`bool`                  | bool            |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`int`                   | int4            |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`long`                  | numeric         | Python 2 only.            |
+--------------------------------+-----------------+---------------------------+
| :class:`str`                   | text            |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`unicode`               | text            | Python 2 only.            |
+--------------------------------+-----------------+---------------------------+
| :class:`float`                 | float8          |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`decimal.Decimal`       | numeric         |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`pg8000.Bytea`          | bytea           | Python 2 only.            |
+--------------------------------+-----------------+---------------------------+
| :class:`bytes`                 | bytea           | Python 3 only.            |
+--------------------------------+-----------------+---------------------------+
| :class:`datetime.datetime`     | timestamp       | ``datetime.datetime.max`` |
| (wo/ tzinfo)                   | without time    | maps to ``infinity``, and |
|                                | zone            | ``datetime.datetime.min`` |
|                                |                 | maps to ``-infinity``.    |
+--------------------------------+-----------------+---------------------------+
| :class:`datetime.datetime`     | timestamp with  | ``datetime.datetime.max`` |
| (w/ tzinfo)                    | time zone       | maps to ``infinity``, and |
|                                |                 | ``datetime.datetime.min`` |
|                                |                 | maps to ``-infinity``.    |
|                                |                 | The max and min datetimes |
|                                |                 | have a UTC timezone.      |
+--------------------------------+-----------------+---------------------------+
| :class:`datetime.date`         | date            | ``datetime.date.max``     |
|                                |                 | maps to ``infinity``, and |
|                                |                 | ``datetime.date.min``     |
|                                |                 | maps to ``-infinity``.    |
+--------------------------------+-----------------+---------------------------+
| :class:`datetime.time`         | time without    |                           |
|                                | time zone       |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`pg8000.Interval`       | interval        |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`uuid.UUID`             | uuid            |                           |
+--------------------------------+-----------------+---------------------------+
| :class:`ipaddress.IPv4Address` | inet            | Python 3.3 onwards        |
+--------------------------------+-----------------+---------------------------+
| :class:`ipaddress.IPv6Address` | inet            | Python 3.3 onwards        |
+--------------------------------+-----------------+---------------------------+
| :class:`ipaddress.IPv4Network` | inet            | Python 3.3 onwards        |
+--------------------------------+-----------------+---------------------------+
| :class:`ipaddress.IPv6Network` | inet            | Python 3.3 onwards        |
+--------------------------------+-----------------+---------------------------+
| None                           | NULL            |                           |
+--------------------------------+-----------------+---------------------------+
| list of :class:`int`           | INT4[]          |                           |
+--------------------------------+-----------------+---------------------------+
| list of :class:`float`         | FLOAT8[]        |                           |
+--------------------------------+-----------------+---------------------------+
| list of :class:`bool`          | BOOL[]          |                           |
+--------------------------------+-----------------+---------------------------+
| list of :class:`str`           | TEXT[]          |                           |
+--------------------------------+-----------------+---------------------------+
| list of :class:`unicode`       | TEXT[]          | Python 2 only.            |
+--------------------------------+-----------------+---------------------------+
