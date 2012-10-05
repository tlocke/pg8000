from ..interface import PreparedStatement
import json
import collections

# TODO: without cursor.description, we need to make our own
# from the data, therefore we must read the entries in the first
# dict in order
json_decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)

# a PG "type code" for a "cursor" type.
# this is totally made up for now.
AKIBAN_NESTED_CURSOR = 5001

def read_datarow(conn, msg, rows, row_desc):
    """Read rows into a row buffer given a
    :class:`.DataRow` and :class:`.RowDescription` object.

    The default implementation of this function just
    populates a single row into "rows".  But here we
    populate lots of rows and also mutate the RowDescription
    to have updated state.

    Needed here is a system whereby Akiban reports to
    us a comprehensive "row description" for a JSON result
    separate from the JSON result itself which includes all
    type information.

    """
    document = json_decoder.decode(msg.fields[0])

    if not document:
        # TODO: would like to put a real cursor.description
        # here, even though no rows
        return
    elif isinstance(document, dict):
        firstrec = document
        document = [document]
    else:
        firstrec = document[0]
    row_desc.fields = _description_from_firstrec(firstrec)

    _create_rowset(conn, document, rows, row_desc.fields)

def _create_rowset(conn, document, rows, fields):
    for rec in document:
        row = []
        for field in fields:
            value = rec[field['name']]
            if field['type_oid'] == AKIBAN_NESTED_CURSOR:
                row.append(NestedCursor(conn, value, field['akiban.description']))
            else:
                row.append(value)
        rows.append(row)

def _description_from_firstrec(firstrec):
    """generate an approximated description given the first record in a
    JSON document.

    """

    ret = []
    for attrnum, (key, value) in enumerate(firstrec.items()):
        rec = {
            'table_oid': None,
            'name': key,
            'column_attrnum': attrnum,
            'format': None,
            'type_modifier': -1,
            'type_size': -1
        }
        rec['type_oid'] = oid = _guess_type(value)
        if oid == AKIBAN_NESTED_CURSOR:
            rec['akiban.description'] = _description_from_firstrec(value[0]) \
                                            if value else ()
        ret.append(rec)
    return ret

def _guess_type(value):
    if isinstance(value, list):
        return AKIBAN_NESTED_CURSOR
    elif isinstance(value, int):
        return 1007
    elif isinstance(value, float):
        return 1022
    elif isinstance(value, basestring):
        return 1043
    elif value is None:
        return 1043
    else:
        assert False, "Don't know what type to use for value: %r" % value

class NestedCursor(object):
    # pg8000 doesn't seem to have this easily settable
    # at the DBAPI level
    arraysize = 1

    def __init__(self, conn, items, description):
        self._row_description = description
        self._rows = collections.deque()
        _create_rowset(conn, items, self._rows, description)

    @property
    def description(self):
        return [
            (col["name"], col["type_oid"],
            None, None, None, None, None)
            for col in self._row_description
        ]

    def fetchone(self):
        if self._rows:
            return self._rows.popleft()
        else:
            return None

    def fetchall(self):
        r = list(self._rows)
        self._rows.clear()
        return r

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        def iter():
            for i, row in enumerate(self.cursor.iterate_tuple()):
                yield row
                if i >= size - 1:
                    break
        return list(iter())


class Extension(object):
    def enable_extension(self, connection):

        enable_json = PreparedStatement(connection.conn,
                                        "set OutputFormat='json'")
        enable_json.execute()
        connection._extensions.append(self)

    def disable_extension(self, connection):
        connection._extensions.remove(self)
        disable_json = PreparedStatement(connection.conn,
                                        "set OutputFormat='sql'")
        disable_json.execute()

    def new_cursor(self, connection, cursor):
        cursor._row_adapter = read_datarow

extension = Extension()