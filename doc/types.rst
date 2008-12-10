:mod:`pg8000.types` --- pg8000 Type Conversion Library
======================================================

.. module:: pg8000.types
    :synopsis: pg8000 Type Conversion Library

Supported Python Types
----------------------

+--------------------------------+-----------------------------+-------+
| Python Type                    | PostgreSQL Type             | Notes |
+================================+=============================+=======+
| bool                           | bool                        |       |
+--------------------------------+-----------------------------+-------+
| int                            | int4                        |       |
+--------------------------------+-----------------------------+-------+
| long                           | numeric                     |       |
+--------------------------------+-----------------------------+-------+
| str                            | text                        |       |
+--------------------------------+-----------------------------+-------+
| unicode                        | text                        |       |
+--------------------------------+-----------------------------+-------+
| float                          | float8                      |       |
+--------------------------------+-----------------------------+-------+
| decimal.Decimal                | numeric                     |       |
+--------------------------------+-----------------------------+-------+
| :class:`pg8000.types.Bytea`    | bytea                       |       |
+--------------------------------+-----------------------------+-------+
| datetime.datetime (wo/ tzinfo) | timestamp without time zone |       |
+--------------------------------+-----------------------------+-------+
| datetime.datetime (w/ tzinfo)  | timestamp with time zone    |       |
+--------------------------------+-----------------------------+-------+
| datetime.date                  | date                        |       |
+--------------------------------+-----------------------------+-------+
| datetime.time                  | time without time zone      |       |
+--------------------------------+-----------------------------+-------+
| :class:`pg8000.types.Interval` | interval                    |       |
+--------------------------------+-----------------------------+-------+
| None                           | NULL                        |       |
+--------------------------------+-----------------------------+-------+
| list of int                    | INT4[]                      |       |
+--------------------------------+-----------------------------+-------+
| list of float                  | FLOAT8[]                    |       |
+--------------------------------+-----------------------------+-------+
| list of bool                   | BOOL[]                      |       |
+--------------------------------+-----------------------------+-------+
| list of str                    | TEXT[]                      |       |
+--------------------------------+-----------------------------+-------+
| list of unicode                | TEXT[]                      |       |
+--------------------------------+-----------------------------+-------+

pg8000 Type Classes
-------------------

.. class:: Bytea(str)

    Bytea is a str-derived class that is mapped to a PostgreSQL byte array.

.. class:: Interval

    An Interval represents a measurement of time.  In PostgreSQL, an interval
    is defined in the measure of months, days, and microseconds; as such, the
    pg8000 interval type represents the same information.

    .. method:: __init__(self, microseconds, days, months)
    
        Initializes an Interval instance with the given values for
        microseconds, days, and months.

    .. attribute:: microseconds

        Measure of microseconds in the interval.

        The microseconds attribute should typically be less than the number of
        microseconds in a day, but there is no restriction ensuring this.
        However, the microseconds value is constrained to fit into a signed
        64-bit integer.  Any attempt to set a value too large or too small
        will result in an OverflowError being raised.

    .. attribute:: days

        Measure of days in the interval.

        The days value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

    .. attribute:: months

        Measure of months in the interval.

        The months value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

