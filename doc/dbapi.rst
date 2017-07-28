.. module:: pg8000

API Reference for pg8000.

Properties
----------
.. autodata:: __version__
   :annotation:

.. autodata:: apilevel
   :annotation:
    
.. autodata:: threadsafety
   :annotation:

.. autodata:: paramstyle
   :annotation:

.. autodata:: STRING
   :annotation:

.. attribute:: BINARY

.. autodata:: NUMBER
   :annotation:

.. autodata:: DATETIME
   :annotation:

.. autodata:: ROWID
   :annotation:


Functions
---------

.. autofunction:: connect

.. autofunction:: Date

.. autofunction:: Time

.. autofunction:: Timestamp

.. autofunction:: DateFromTicks

.. autofunction:: TimeFromTicks

.. autofunction:: TimestampFromTicks

.. autofunction:: Binary


Generic Exceptions
------------------
pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions.
Generally, more specific exception types are raised; these specific exception
types are derived from the generic exceptions.

.. autoexception:: Warning

.. autoexception:: Error

.. autoexception:: InterfaceError

.. autoexception:: DatabaseError

.. autoexception:: DataError

.. autoexception:: OperationalError

.. autoexception:: IntegrityError

.. autoexception:: InternalError

.. autoexception:: ProgrammingError

.. autoexception:: NotSupportedError


Specific Exceptions
-------------------
    
Exceptions that are subclassed from the standard DB-API 2.0 exceptions above.

.. autoexception:: ArrayContentNotSupportedError

.. autoexception:: ArrayContentNotHomogenousError

.. autoexception:: ArrayDimensionsNotConsistentError


Classes
-------

.. autoclass:: Connection()
   :members:

.. autoclass:: Cursor()
   :members:


Type Classes
------------

.. autoclass:: Bytea

.. autoclass:: Interval
