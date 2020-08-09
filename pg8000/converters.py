from datetime import (
    date as Date, datetime as Datetime, time as Time, timedelta as Timedelta,
    timezone as Timezone)
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address, IPv4Network, IPv6Address, IPv6Network, ip_address, ip_network)
from json import loads
from time import localtime
from uuid import UUID

from pg8000.exceptions import InterfaceError


BIGINTEGER = 20
BOOLEAN = 16
BOOLEAN_ARRAY = 1000
BYTES = 17
CHAR = 1042
CHAR_ARRAY = 1014
DATE = 1082
DATETIME = 1114
DECIMAL = 1700
DECIMAL_ARRAY = 1231
FLOAT = 701
FLOAT_ARRAY = 1022
INET = 869
INT2VECTOR = 22
INTEGER = 23
INTEGER_ARRAY = 1016
INTERVAL = 1186
MACADDR = 829
NAME = 19
NAME_ARRAY = 1003
OID = 26
JSON = 114
JSONB = 3802
NULLTYPE = -1
NUMBER = 1700
ROWID = 26
STRING = 1043
TEXT = 25
TEXT_ARRAY = 1009
TIME = 1083
TIMEDELTA = 1186
TIMESTAMP = 1114
TIMESTAMPTZ = 1184
UNKNOWN = 705
UUID_TYPE = 2950
VARCHAR = 1043
VARCHAR_ARRAY = 1015
XID = 28


MIN_INT2, MAX_INT2 = -2 ** 15, 2 ** 15
MIN_INT4, MAX_INT4 = -2 ** 31, 2 ** 31
MIN_INT8, MAX_INT8 = -2 ** 63, 2 ** 63


def text_out(v):
    return v


def enum_out(v):
    return str(v.value)


def time_out(v):
    return v.isoformat()


def date_out(v):
    return v.isoformat()


def unknown_out(v):
    return str(v)


def vector_in(data):
    return eval('[' + data.replace(' ', ',') + ']')


def text_in(data):
    return data


def bool_in(data):
    return data == 't'


def json_in(data):
    return loads(data)


def time_in(data):
    if "." in data:
        pattern = "%H:%M:%S.%f"
    else:
        pattern = "%H:%M:%S"

    return Datetime.strptime(data, pattern).time()


def date_in(data):
    return Datetime.strptime(data, "%Y-%m-%d").date()


def numeric_in(data):
    return Decimal(data)


def numeric_out(d):
    return str(d)


def inet_out(v):
    return str(v)


def inet_in(data):
    return ip_network(data, False) if '/' in data else ip_address(data)


def int_out(v):
    return str(v)


def float_out(v):
    return str(v)


def timestamp_in(data):
    if data in ('infinity', '-infinity'):
        return data

    elif '.' in data:
        pattern = "%Y-%m-%d %H:%M:%S.%f"

    else:
        pattern = "%Y-%m-%d %H:%M:%S"

    return Datetime.strptime(data, pattern)


def timestamp_out(v):
    return v.isoformat()


def timestamptz_out(v):
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return v.astimezone(Timezone.utc).isoformat()


def timestamptz_in(data):
    patt = "%Y-%m-%d %H:%M:%S.%f%z" if '.' in data else "%Y-%m-%d %H:%M:%S%z"
    return Datetime.strptime(data + '00', patt)


def pginterval_out(v):
    return str(v)


def timedelta_out(v):
    return ' '.join(
        (
            str(v.days), "days", str(v.seconds), "seconds",
            str(v.microseconds), "microseconds"
        )
    )


def timedelta_in(data):
    t = {}

    curr_val = None
    for k in data.split():
        if ':' in k:
            t['hours'], t['minutes'], t['seconds'] = map(float, k.split(':'))
        else:
            try:
                curr_val = float(k)
            except ValueError:
                t[PGInterval.UNIT_MAP[k]] = curr_val

    for n in ['weeks', 'months', 'years', 'decades', 'centuries', 'millennia']:
        if n in t:
            raise InterfaceError(
                "Can't fit the interval " + str(t) +
                " into a datetime.timedelta.")

    return Timedelta(**t)


def pginterval_in(data):
    return PGInterval.from_str(data)


def bytes_out(v):
    return '\\x' + v.hex()


def bytea_in(data):
    return bytes.fromhex(data[2:])


def uuid_out(v):
    return str(v)


def uuid_in(data):
    return UUID(data)


def bool_out(v):
    return 'true' if v else 'false'


def null_out(v):
    return None


def int_in(data):
    return int(data)


BINARY = bytes


def PgDate(year, month, day):
    """Constuct an object holding a date value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`
    """
    return Date(year, month, day)


def PgTime(hour, minute, second):
    """Construct an object holding a time value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`
    """
    return Time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    """Construct an object holding a timestamp value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`
    """
    return Datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """Construct an object holding a date value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`
    """
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Construct an objet holding a time value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`
    """
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Construct an object holding a timestamp value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`
    """
    return Timestamp(*localtime(ticks)[:6])


def Binary(value):
    """Construct an object holding binary data.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    """
    return value


class PGInterval():
    UNIT_MAP = {
        'year': 'years',
        'years': 'years',
        'millennia': 'millennia',
        'millenium': 'millennia',
        'centuries': 'centuries',
        'century': 'centuries',
        'decades': 'decades',
        'decade': 'decades',
        'years': 'years',
        'year': 'years',
        'months': 'months',
        'month': 'months',
        'mon': 'months',
        'mons': 'months',
        'weeks': 'weeks',
        'week': 'weeks',
        'days': 'days',
        'day': 'days',
        'hours': 'hours',
        'hour': 'hours',
        'minutes': 'minutes',
        'minute': 'minutes',
        'seconds': 'seconds',
        'second': 'seconds',
        'microseconds': 'microseconds',
        'microsecond': 'microseconds'
    }

    @staticmethod
    def from_str(interval_str):
        t = {}

        curr_val = None
        for k in interval_str.split():
            if ':' in k:
                hours_str, minutes_str, seconds_str = k.split(':')
                hours = int(hours_str)
                if hours != 0:
                    t['hours'] = hours
                minutes = int(minutes_str)
                if minutes != 0:
                    t['minutes'] = minutes
                try:
                    seconds = int(seconds_str)
                except ValueError:
                    seconds = float(seconds_str)

                if seconds != 0:
                    t['seconds'] = seconds

            else:
                try:
                    curr_val = int(k)
                except ValueError:
                    t[PGInterval.UNIT_MAP[k]] = curr_val

        return PGInterval(**t)

    def __init__(
            self, millennia=None, centuries=None, decades=None, years=None,
            months=None, weeks=None, days=None, hours=None, minutes=None,
            seconds=None, microseconds=None):
        self.millennia = millennia
        self.centuries = centuries
        self.decades = decades
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.microseconds = microseconds

    def __repr__(self):
        res = ["<PGInterval"]
        for name, value in (
                ('millennia', self.millennia),
                ('centuries', self.centuries),
                ('decades', self.decades),
                ('years', self.years),
                ('months', self.months),
                ('weeks', self.weeks),
                ('days', self.days),
                ('hours', self.hours),
                ('minutes', self.minutes),
                ('seconds', self.seconds),
                ('microseconds', self.microseconds)):
            if value is not None:
                res.append(str(value))
                res.append(name)

        return ' '.join(res) + '>'

    def __str__(self):
        res = []
        if self.millennia is not None:
            res.append(str(self.millenia))
            res.append('millenia')

        if self.centuries is not None:
            res.append(str(self.centuries))
            res.append('centuries')

        if self.decades is not None:
            res.append(str(self.decades))
            res.append('decades')

        if self.years is not None:
            res.append(str(self.years))
            res.append('years')

        if self.months is not None:
            res.append(str(self.months))
            res.append('months')

        if self.weeks is not None:
            res.append(str(self.weeks))
            res.append('weeks')

        if self.days is not None:
            res.append(str(self.days))
            res.append('days')

        if self.hours is not None:
            res.append(str(self.hours))
            res.append('hours')

        if self.minutes is not None:
            res.append(str(self.minutes))
            res.append('minutes')

        if self.seconds is not None:
            res.append(str(self.seconds))
            res.append('seconds')

        if self.microseconds is not None:
            res.append(str(self.microseconds))
            res.append('microseconds')

        return ' '.join(res)

    def normalize(self):
        months = 0
        if self.months is not None:
            months += self.months
        if self.years is not None:
            months += self.years * 12

        days = 0
        if self.days is not None:
            days += self.days
        if self.weeks is not None:
            days += self.weeks * 7

        seconds = 0
        if self.hours is not None:
            seconds += self.hours * 60 * 60
        if self.minutes is not None:
            seconds += self.minutes * 60
        if self.seconds is not None:
            seconds += self.seconds
        if self.microseconds is not None:
            seconds += self.microseconds / 1000000

        return PGInterval(months=months, days=days, seconds=seconds)

    def __eq__(self, other):
        if isinstance(other, PGInterval):
            s = self.normalize()
            o = other.normalize()
            return s.months == o.months and s.days == o.days and \
                s.seconds == o.seconds
        else:
            return False


class ArrayState(Enum):
    InString = 1
    InEscape = 2
    InValue = 3
    Out = 4


def _parse_array(data, adapter):
    state = ArrayState.Out
    stack = [[]]
    val = []
    for c in data:
        if state == ArrayState.InValue:
            if c in ('}', ','):
                value = ''.join(val)
                stack[-1].append(None if value == 'NULL' else adapter(value))
                state = ArrayState.Out
            else:
                val.append(c)

        if state == ArrayState.Out:
            if c == '{':
                a = []
                stack[-1].append(a)
                stack.append(a)
            elif c == '}':
                stack.pop()
            elif c == ',':
                pass
            elif c == '"':
                val = []
                state = ArrayState.InString
            else:
                val = [c]
                state = ArrayState.InValue

        elif state == ArrayState.InString:
            if c == '"':
                stack[-1].append(adapter(''.join(val)))
                state = ArrayState.Out
            elif c == '\\':
                state = ArrayState.InEscape
            else:
                val.append(c)
        elif state == ArrayState.InEscape:
            val.append(c)
            state = ArrayState.InString

    return stack[0][0]


def _array_in(adapter):
    def f(data):
        return _parse_array(data, adapter)
    return f


array_bool_in = _array_in(bool_in)
array_int_in = _array_in(int)
array_float_in = _array_in(float)
array_numeric_in = _array_in(numeric_in)
array_text_in = _array_in(text_in)


def array_string_escape(v):
    cs = []
    for c in v:
        if c == '\\':
            cs.append('\\')
        elif c == '"':
            cs.append('\\')
        cs.append(c)
    val = ''.join(cs)
    if len(val) == 0 or val == 'NULL' or any(
            [c in val for c in ('{', '}', ",", " ", '\\')]):
        val = '"' + val + '"'
    return val


PY_TYPES = {
    type(None): (NULLTYPE, null_out),  # null
    bool: (16, bool_out),
    bytearray: (17, bytes_out),  # bytea
    20: (20, int_out),  # int8
    21: (21, int_out),  # int2
    23: (23, int_out),  # int4
    float: (701, float_out),  # float8
    Date: (1082, date_out),  # date
    Time: (1083, time_out),  # time
    1114: (1114, timestamp_out),  # timestamp
    1184: (1184, timestamptz_out),  # timestamptz
    Timedelta: (1186, timedelta_out),
    PGInterval: (1186, pginterval_out),
    Decimal: (1700, numeric_out),  # Decimal
    UUID: (2950, uuid_out),  # uuid
    bytes: (17, bytes_out),  # bytea
    str: (UNKNOWN, text_out),  # unknown
    Enum: (UNKNOWN, enum_out),
    IPv4Address: (869, inet_out),  # inet
    IPv6Address: (869, inet_out),  # inet
    IPv4Network: (869, inet_out),  # inet
    IPv6Network: (869, inet_out),  # inet
}


PG_TYPES = {
    16: bool_in,  # boolean
    17: bytea_in,  # bytea
    19: text_in,  # name type
    20: int,  # int8
    21: int,  # int2
    22: vector_in,  # int2vector
    23: int,  # int4
    25: text_in,  # TEXT type
    26: int,  # oid
    28: int,  # xid
    114: json_in,  # json
    700: float,  # float4
    701: float,  # float8
    UNKNOWN: text_in,  # unknown
    829: text_in,  # MACADDR type
    869: inet_in,  # inet
    1000: array_bool_in,  # BOOL[]
    1003: array_text_in,  # NAME[]
    1005: array_int_in,  # INT2[]
    1007: array_int_in,  # INT4[]
    1009: array_text_in,  # TEXT[]
    1014: array_text_in,  # CHAR[]
    1015: array_text_in,  # VARCHAR[]
    1016: array_int_in,  # INT8[]
    1021: array_float_in,  # FLOAT4[]
    1022: array_float_in,  # FLOAT8[]
    1042: text_in,  # CHAR type
    1043: text_in,  # VARCHAR type
    1082: date_in,  # date
    1083: time_in,
    1114: timestamp_in,
    1184: timestamptz_in,  # timestamp w/ tz
    1186: timedelta_in,
    1231: array_numeric_in,  # NUMERIC[]
    1263: array_text_in,  # cstring[]
    1700: numeric_in,  # NUMERIC
    2275: text_in,  # cstring
    2950: uuid_in,  # uuid
    3802: json_in,  # jsonb
}


# PostgreSQL encodings:
# https://www.postgresql.org/docs/current/multibyte.html
#
# Python encodings:
# https://docs.python.org/3/library/codecs.html
#
# Commented out encodings don't require a name change between PostgreSQL and
# Python.  If the py side is None, then the encoding isn't supported.
pg_to_py_encodings = {
    # Not supported:
    "mule_internal": None,
    "euc_tw": None,

    # Name fine as-is:
    # "euc_jp",
    # "euc_jis_2004",
    # "euc_kr",
    # "gb18030",
    # "gbk",
    # "johab",
    # "sjis",
    # "shift_jis_2004",
    # "uhc",
    # "utf8",

    # Different name:
    "euc_cn": "gb2312",
    "iso_8859_5": "is8859_5",
    "iso_8859_6": "is8859_6",
    "iso_8859_7": "is8859_7",
    "iso_8859_8": "is8859_8",
    "koi8": "koi8_r",
    "latin1": "iso8859-1",
    "latin2": "iso8859_2",
    "latin3": "iso8859_3",
    "latin4": "iso8859_4",
    "latin5": "iso8859_9",
    "latin6": "iso8859_10",
    "latin7": "iso8859_13",
    "latin8": "iso8859_14",
    "latin9": "iso8859_15",
    "sql_ascii": "ascii",
    "win866": "cp886",
    "win874": "cp874",
    "win1250": "cp1250",
    "win1251": "cp1251",
    "win1252": "cp1252",
    "win1253": "cp1253",
    "win1254": "cp1254",
    "win1255": "cp1255",
    "win1256": "cp1256",
    "win1257": "cp1257",
    "win1258": "cp1258",
    "unicode": "utf-8",  # Needed for Amazon Redshift
}

# pg element oid -> pg array typeoid
PG_ARRAY_TYPES = {
    16: 1000,
    25: 1009,    # TEXT[]
    701: 1022,
    1043: 1009,
    1700: 1231,  # NUMERIC[]
}
