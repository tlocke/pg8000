import time
import warnings

import pytest

import pg8000

""" Python DB API 2.0 driver compliance unit test suite.

    This software is Public Domain and may be used without restrictions.

 "Now we have booze and barflies entering the discussion, plus rumours of
  DBAs on drugs... and I won't tell you what flashes through my mind each
  time I read the subject line with 'Anal Compliance' in it.  All around
  this is turning out to be a thoroughly unwholesome unit test."

    -- Ian Bicking
"""

__rcs_id__ = "$Id: dbapi20.py,v 1.10 2003/10/09 03:14:14 zenzen Exp $"
__version__ = "$Revision: 1.10 $"[11:-2]
__author__ = "Stuart Bishop <zen@shangri-la.dropbear.id.au>"


# $Log: dbapi20.py,v $
# Revision 1.10  2003/10/09 03:14:14  zenzen
# Add test for DB API 2.0 optional extension, where database exceptions
# are exposed as attributes on the Connection object.
#
# Revision 1.9  2003/08/13 01:16:36  zenzen
# Minor tweak from Stefan Fleiter
#
# Revision 1.8  2003/04/10 00:13:25  zenzen
# Changes, as per suggestions by M.-A. Lemburg
# - Add a table prefix, to ensure namespace collisions can always be avoided
#
# Revision 1.7  2003/02/26 23:33:37  zenzen
# Break out DDL into helper functions, as per request by David Rushby
#
# Revision 1.6  2003/02/21 03:04:33  zenzen
# Stuff from Henrik Ekelund:
#     added test_None
#     added test_nextset & hooks
#
# Revision 1.5  2003/02/17 22:08:43  zenzen
# Implement suggestions and code from Henrik Eklund - test that
# cursor.arraysize defaults to 1 & generic cursor.callproc test added
#
# Revision 1.4  2003/02/15 00:16:33  zenzen
# Changes, as per suggestions and bug reports by M.-A. Lemburg,
# Matthew T. Kromer, Federico Di Gregorio and Daniel Dittmar
# - Class renamed
# - Now a subclass of TestCase, to avoid requiring the driver stub
#   to use multiple inheritance
# - Reversed the polarity of buggy test in test_description
# - Test exception heirarchy correctly
# - self.populate is now self._populate(), so if a driver stub
#   overrides self.ddl1 this change propogates
# - VARCHAR columns now have a width, which will hopefully make the
#   DDL even more portible (this will be reversed if it causes more problems)
# - cursor.rowcount being checked after various execute and fetchXXX methods
# - Check for fetchall and fetchmany returning empty lists after results
#   are exhausted (already checking for empty lists if select retrieved
#   nothing
# - Fix bugs in test_setoutputsize_basic and test_setinputsizes
#


""" Test a database self.driver for DB API 2.0 compatibility.
    This implementation tests Gadfly, but the TestCase
    is structured so that other self.drivers can subclass this
    test case to ensure compiliance with the DB-API. It is
    expected that this TestCase may be expanded in the future
    if ambiguities or edge conditions are discovered.

    The 'Optional Extensions' are not yet being tested.

    self.drivers should subclass this test, overriding setUp, tearDown,
    self.driver, connect_args and connect_kw_args. Class specification
    should be as follows:

    import dbapi20
    class mytest(dbapi20.DatabaseAPI20Test):
       [...]

    Don't 'import DatabaseAPI20Test from dbapi20', or you will
    confuse the unit tester - just 'import dbapi20'.
"""

# The self.driver module. This should be the module where the 'connect'
# method is to be found
driver = pg8000
table_prefix = "dbapi20test_"  # If you need to specify a prefix for tables

ddl1 = "create table %sbooze (name varchar(20))" % table_prefix
ddl2 = "create table %sbarflys (name varchar(20))" % table_prefix
xddl1 = "drop table %sbooze" % table_prefix
xddl2 = "drop table %sbarflys" % table_prefix

# Name of stored procedure to convert
# string->lowercase
lowerfunc = "lower"


# Some drivers may need to override these helpers, for example adding
# a 'commit' after the execute.
def executeDDL1(cursor):
    cursor.execute(ddl1)


def executeDDL2(cursor):
    cursor.execute(ddl2)


@pytest.fixture
def db(request, con):
    def fin():
        with con.cursor() as cur:
            for ddl in (xddl1, xddl2):
                try:
                    cur.execute(ddl)
                    con.commit()
                except driver.Error:
                    # Assume table didn't exist. Other tests will check if
                    # execute is busted.
                    pass

    request.addfinalizer(fin)
    return con


def test_apilevel():
    # Must exist
    apilevel = driver.apilevel

    # Must equal 2.0
    assert apilevel == "2.0"


def test_threadsafety():
    try:
        # Must exist
        threadsafety = driver.threadsafety
        # Must be a valid value
        assert threadsafety in (0, 1, 2, 3)
    except AttributeError:
        assert False, "Driver doesn't define threadsafety"


def test_paramstyle():
    try:
        # Must exist
        paramstyle = driver.paramstyle
        # Must be a valid value
        assert paramstyle in ("qmark", "numeric", "named", "format", "pyformat")
    except AttributeError:
        assert False, "Driver doesn't define paramstyle"


def test_Exceptions():
    # Make sure required exceptions exist, and are in the
    # defined heirarchy.
    assert issubclass(driver.Warning, Exception)
    assert issubclass(driver.Error, Exception)
    assert issubclass(driver.InterfaceError, driver.Error)
    assert issubclass(driver.DatabaseError, driver.Error)
    assert issubclass(driver.OperationalError, driver.Error)
    assert issubclass(driver.IntegrityError, driver.Error)
    assert issubclass(driver.InternalError, driver.Error)
    assert issubclass(driver.ProgrammingError, driver.Error)
    assert issubclass(driver.NotSupportedError, driver.Error)


def test_ExceptionsAsConnectionAttributes(con):
    # OPTIONAL EXTENSION
    # Test for the optional DB API 2.0 extension, where the exceptions
    # are exposed as attributes on the Connection object
    # I figure this optional extension will be implemented by any
    # driver author who is using this test suite, so it is enabled
    # by default.
    warnings.simplefilter("ignore")
    drv = driver
    assert con.Warning is drv.Warning
    assert con.Error is drv.Error
    assert con.InterfaceError is drv.InterfaceError
    assert con.DatabaseError is drv.DatabaseError
    assert con.OperationalError is drv.OperationalError
    assert con.IntegrityError is drv.IntegrityError
    assert con.InternalError is drv.InternalError
    assert con.ProgrammingError is drv.ProgrammingError
    assert con.NotSupportedError is drv.NotSupportedError
    warnings.resetwarnings()


def test_commit(con):
    # Commit must work, even if it doesn't do anything
    con.commit()


def test_rollback(con):
    # If rollback is defined, it should either work or throw
    # the documented exception
    if hasattr(con, "rollback"):
        try:
            con.rollback()
        except driver.NotSupportedError:
            pass


def test_cursor(con):
    con.cursor()


def test_cursor_isolation(con):
    # Make sure cursors created from the same connection have
    # the documented transaction isolation level
    cur1 = con.cursor()
    cur2 = con.cursor()
    executeDDL1(cur1)
    cur1.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    cur2.execute("select name from %sbooze" % table_prefix)
    booze = cur2.fetchall()
    assert len(booze) == 1
    assert len(booze[0]) == 1
    assert booze[0][0] == "Victoria Bitter"


def test_description(con):
    cur = con.cursor()
    executeDDL1(cur)
    assert cur.description is None, (
        "cursor.description should be none after executing a "
        "statement that can return no rows (such as DDL)"
    )
    cur.execute("select name from %sbooze" % table_prefix)
    assert len(cur.description) == 1, "cursor.description describes too many columns"
    assert (
        len(cur.description[0]) == 7
    ), "cursor.description[x] tuples must have 7 elements"
    assert (
        cur.description[0][0].lower() == "name"
    ), "cursor.description[x][0] must return column name"
    assert cur.description[0][1] == driver.STRING, (
        "cursor.description[x][1] must return column type. Got %r"
        % cur.description[0][1]
    )

    # Make sure self.description gets reset
    executeDDL2(cur)
    assert cur.description is None, (
        "cursor.description not being set to None when executing "
        "no-result statements (eg. DDL)"
    )


def test_rowcount(cursor):
    executeDDL1(cursor)
    assert cursor.rowcount == -1, (
        "cursor.rowcount should be -1 after executing no-result " "statements"
    )
    cursor.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    assert cursor.rowcount in (-1, 1), (
        "cursor.rowcount should == number or rows inserted, or "
        "set to -1 after executing an insert statement"
    )
    cursor.execute("select name from %sbooze" % table_prefix)
    assert cursor.rowcount in (-1, 1), (
        "cursor.rowcount should == number of rows returned, or "
        "set to -1 after executing a select statement"
    )
    executeDDL2(cursor)
    assert cursor.rowcount == -1, (
        "cursor.rowcount not being reset to -1 after executing " "no-result statements"
    )


def test_close(con):
    cur = con.cursor()
    con.close()

    # cursor.execute should raise an Error if called after connection
    # closed
    with pytest.raises(driver.Error):
        executeDDL1(cur)

    # connection.commit should raise an Error if called after connection'
    # closed.'
    with pytest.raises(driver.Error):
        con.commit()

    # connection.close should raise an Error if called more than once
    with pytest.raises(driver.Error):
        con.close()


def test_execute(con):
    cur = con.cursor()
    _paraminsert(cur)


def _paraminsert(cur):
    executeDDL1(cur)
    cur.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    assert cur.rowcount in (-1, 1)

    if driver.paramstyle == "qmark":
        cur.execute("insert into %sbooze values (?)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "numeric":
        cur.execute("insert into %sbooze values (:1)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "named":
        cur.execute(
            "insert into %sbooze values (:beer)" % table_prefix, {"beer": "Cooper's"}
        )
    elif driver.paramstyle == "format":
        cur.execute("insert into %sbooze values (%%s)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "pyformat":
        cur.execute(
            "insert into %sbooze values (%%(beer)s)" % table_prefix,
            {"beer": "Cooper's"},
        )
    else:
        assert False, "Invalid paramstyle"

    assert cur.rowcount in (-1, 1)

    cur.execute("select name from %sbooze" % table_prefix)
    res = cur.fetchall()
    assert len(res) == 2, "cursor.fetchall returned too few rows"
    beers = [res[0][0], res[1][0]]
    beers.sort()
    assert beers[0] == "Cooper's", (
        "cursor.fetchall retrieved incorrect data, or data inserted " "incorrectly"
    )
    assert beers[1] == "Victoria Bitter", (
        "cursor.fetchall retrieved incorrect data, or data inserted " "incorrectly"
    )


def test_executemany(cursor):
    executeDDL1(cursor)
    largs = [("Cooper's",), ("Boag's",)]
    margs = [{"beer": "Cooper's"}, {"beer": "Boag's"}]
    if driver.paramstyle == "qmark":
        cursor.executemany("insert into %sbooze values (?)" % table_prefix, largs)
    elif driver.paramstyle == "numeric":
        cursor.executemany("insert into %sbooze values (:1)" % table_prefix, largs)
    elif driver.paramstyle == "named":
        cursor.executemany("insert into %sbooze values (:beer)" % table_prefix, margs)
    elif driver.paramstyle == "format":
        cursor.executemany("insert into %sbooze values (%%s)" % table_prefix, largs)
    elif driver.paramstyle == "pyformat":
        cursor.executemany(
            "insert into %sbooze values (%%(beer)s)" % (table_prefix), margs
        )
    else:
        assert False, "Unknown paramstyle"

    assert cursor.rowcount in (-1, 2), (
        "insert using cursor.executemany set cursor.rowcount to "
        "incorrect value %r" % cursor.rowcount
    )

    cursor.execute("select name from %sbooze" % table_prefix)
    res = cursor.fetchall()
    assert len(res) == 2, "cursor.fetchall retrieved incorrect number of rows"
    beers = [res[0][0], res[1][0]]
    beers.sort()
    assert beers[0] == "Boag's", "incorrect data retrieved"
    assert beers[1] == "Cooper's", "incorrect data retrieved"


def test_fetchone(cursor):
    # cursor.fetchone should raise an Error if called before
    # executing a select-type query
    with pytest.raises(driver.Error):
        cursor.fetchone()

    # cursor.fetchone should raise an Error if called after
    # executing a query that cannnot return rows
    executeDDL1(cursor)
    with pytest.raises(driver.Error):
        cursor.fetchone()

    cursor.execute("select name from %sbooze" % table_prefix)
    assert cursor.fetchone() is None, (
        "cursor.fetchone should return None if a query retrieves " "no rows"
    )
    assert cursor.rowcount in (-1, 0)

    # cursor.fetchone should raise an Error if called after
    # executing a query that cannnot return rows
    cursor.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    with pytest.raises(driver.Error):
        cursor.fetchone()

    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchone()
    assert len(r) == 1, "cursor.fetchone should have retrieved a single row"
    assert r[0] == "Victoria Bitter", "cursor.fetchone retrieved incorrect data"
    assert (
        cursor.fetchone() is None
    ), "cursor.fetchone should return None if no more rows available"
    assert cursor.rowcount in (-1, 1)


samples = [
    "Carlton Cold",
    "Carlton Draft",
    "Mountain Goat",
    "Redback",
    "Victoria Bitter",
    "XXXX",
]


def _populate():
    """Return a list of sql commands to setup the DB for the fetch
    tests.
    """
    populate = [
        "insert into %sbooze values ('%s')" % (table_prefix, s) for s in samples
    ]
    return populate


def test_fetchmany(cursor):
    # cursor.fetchmany should raise an Error if called without
    # issuing a query
    with pytest.raises(driver.Error):
        cursor.fetchmany(4)

    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchmany()
    assert len(r) == 1, (
        "cursor.fetchmany retrieved incorrect number of rows, "
        "default of arraysize is one."
    )
    cursor.arraysize = 10
    r = cursor.fetchmany(3)  # Should get 3 rows
    assert len(r) == 3, "cursor.fetchmany retrieved incorrect number of rows"
    r = cursor.fetchmany(4)  # Should get 2 more
    assert len(r) == 2, "cursor.fetchmany retrieved incorrect number of rows"
    r = cursor.fetchmany(4)  # Should be an empty sequence
    assert len(r) == 0, (
        "cursor.fetchmany should return an empty sequence after "
        "results are exhausted"
    )
    assert cursor.rowcount in (-1, 6)

    # Same as above, using cursor.arraysize
    cursor.arraysize = 4
    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchmany()  # Should get 4 rows
    assert len(r) == 4, "cursor.arraysize not being honoured by fetchmany"
    r = cursor.fetchmany()  # Should get 2 more
    assert len(r) == 2
    r = cursor.fetchmany()  # Should be an empty sequence
    assert len(r) == 0
    assert cursor.rowcount in (-1, 6)

    cursor.arraysize = 6
    cursor.execute("select name from %sbooze" % table_prefix)
    rows = cursor.fetchmany()  # Should get all rows
    assert cursor.rowcount in (-1, 6)
    assert len(rows) == 6
    assert len(rows) == 6
    rows = [row[0] for row in rows]
    rows.sort()

    # Make sure we get the right data back out
    for i in range(0, 6):
        assert rows[i] == samples[i], "incorrect data retrieved by cursor.fetchmany"

    rows = cursor.fetchmany()  # Should return an empty list
    assert len(rows) == 0, (
        "cursor.fetchmany should return an empty sequence if "
        "called after the whole result set has been fetched"
    )
    assert cursor.rowcount in (-1, 6)

    executeDDL2(cursor)
    cursor.execute("select name from %sbarflys" % table_prefix)
    r = cursor.fetchmany()  # Should get empty sequence
    assert len(r) == 0, (
        "cursor.fetchmany should return an empty sequence if " "query retrieved no rows"
    )
    assert cursor.rowcount in (-1, 0)


def test_fetchall(cursor):
    # cursor.fetchall should raise an Error if called
    # without executing a query that may return rows (such
    # as a select)
    with pytest.raises(driver.Error):
        cursor.fetchall()

    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    # cursor.fetchall should raise an Error if called
    # after executing a a statement that cannot return rows
    with pytest.raises(driver.Error):
        cursor.fetchall()

    cursor.execute("select name from %sbooze" % table_prefix)
    rows = cursor.fetchall()
    assert cursor.rowcount in (-1, len(samples))
    assert len(rows) == len(samples), "cursor.fetchall did not retrieve all rows"
    rows = [r[0] for r in rows]
    rows.sort()
    for i in range(0, len(samples)):
        assert rows[i] == samples[i], "cursor.fetchall retrieved incorrect rows"
    rows = cursor.fetchall()
    assert len(rows) == 0, (
        "cursor.fetchall should return an empty list if called "
        "after the whole result set has been fetched"
    )
    assert cursor.rowcount in (-1, len(samples))

    executeDDL2(cursor)
    cursor.execute("select name from %sbarflys" % table_prefix)
    rows = cursor.fetchall()
    assert cursor.rowcount in (-1, 0)
    assert len(rows) == 0, (
        "cursor.fetchall should return an empty list if "
        "a select query returns no rows"
    )


def test_mixedfetch(cursor):
    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    cursor.execute("select name from %sbooze" % table_prefix)
    rows1 = cursor.fetchone()
    rows23 = cursor.fetchmany(2)
    rows4 = cursor.fetchone()
    rows56 = cursor.fetchall()
    assert cursor.rowcount in (-1, 6)
    assert len(rows23) == 2, "fetchmany returned incorrect number of rows"
    assert len(rows56) == 2, "fetchall returned incorrect number of rows"

    rows = [rows1[0]]
    rows.extend([rows23[0][0], rows23[1][0]])
    rows.append(rows4[0])
    rows.extend([rows56[0][0], rows56[1][0]])
    rows.sort()
    for i in range(0, len(samples)):
        assert rows[i] == samples[i], "incorrect data retrieved or inserted"


def help_nextset_setUp(cur):
    """Should create a procedure called deleteme
    that returns two result sets, first the
    number of rows in booze then "name from booze"
    """
    raise NotImplementedError("Helper not implemented")


def help_nextset_tearDown(cur):
    "If cleaning up is needed after nextSetTest"
    raise NotImplementedError("Helper not implemented")


def test_nextset(cursor):
    if not hasattr(cursor, "nextset"):
        return

    try:
        executeDDL1(cursor)
        sql = _populate()
        for sql in _populate():
            cursor.execute(sql)

        help_nextset_setUp(cursor)

        cursor.callproc("deleteme")
        numberofrows = cursor.fetchone()
        assert numberofrows[0] == len(samples)
        assert cursor.nextset()
        names = cursor.fetchall()
        assert len(names) == len(samples)
        s = cursor.nextset()
        assert s is None, "No more return sets, should return None"
    finally:
        help_nextset_tearDown(cursor)


def test_arraysize(cursor):
    # Not much here - rest of the tests for this are in test_fetchmany
    assert hasattr(cursor, "arraysize"), "cursor.arraysize must be defined"


def test_setinputsizes(cursor):
    cursor.setinputsizes(25)


def test_setoutputsize_basic(cursor):
    # Basic test is to make sure setoutputsize doesn't blow up
    cursor.setoutputsize(1000)
    cursor.setoutputsize(2000, 0)
    _paraminsert(cursor)  # Make sure the cursor still works


def test_None(cursor):
    executeDDL1(cursor)
    cursor.execute("insert into %sbooze values (NULL)" % table_prefix)
    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchall()
    assert len(r) == 1
    assert len(r[0]) == 1
    assert r[0][0] is None, "NULL value not returned as None"


def test_Date():
    driver.Date(2002, 12, 25)
    driver.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
    # Can we assume this? API doesn't specify, but it seems implied
    # self.assertEqual(str(d1),str(d2))


def test_Time():
    driver.Time(13, 45, 30)
    driver.TimeFromTicks(time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))
    # Can we assume this? API doesn't specify, but it seems implied
    # self.assertEqual(str(t1),str(t2))


def test_Timestamp():
    driver.Timestamp(2002, 12, 25, 13, 45, 30)
    driver.TimestampFromTicks(time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0)))
    # Can we assume this? API doesn't specify, but it seems implied
    # self.assertEqual(str(t1),str(t2))


def test_Binary():
    driver.Binary(b"Something")
    driver.Binary(b"")


def test_STRING():
    assert hasattr(driver, "STRING"), "module.STRING must be defined"


def test_BINARY():
    assert hasattr(driver, "BINARY"), "module.BINARY must be defined."


def test_NUMBER():
    assert hasattr(driver, "NUMBER"), "module.NUMBER must be defined."


def test_DATETIME():
    assert hasattr(driver, "DATETIME"), "module.DATETIME must be defined."


def test_ROWID():
    assert hasattr(driver, "ROWID"), "module.ROWID must be defined."
