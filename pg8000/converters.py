from datetime import (
    date as Date, datetime as Datetime, time as Time, timedelta as Timedelta,
    timezone as Timezone)
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address, IPv4Network, IPv6Address, IPv6Network, ip_address, ip_network)
from json import dumps, loads
from uuid import UUID

from pg8000.exceptions import InterfaceError


ANY_ARRAY = 2277
BIGINT = 20
BIGINT_ARRAY = 1016
BOOLEAN = 16
BOOLEAN_ARRAY = 1000
BYTES = 17
BYTES_ARRAY = 1001
CHAR = 1042
CHAR_ARRAY = 1014
CIDR = 650
CIDR_ARRAY = 651
CSTRING = 2275
CSTRING_ARRAY = 1263
DATE = 1082
DATE_ARRAY = 1182
DATETIME = 1114
FLOAT = 701
FLOAT_ARRAY = 1022
INET = 869
INET_ARRAY = 1041
INT2VECTOR = 22
INTEGER = 23
INTEGER_ARRAY = 1007
INTERVAL = 1186
OID = 26
JSON = 114
JSON_ARRAY = 199
JSONB = 3802
JSONB_ARRAY = 3807
MACADDR = 829
MONEY = 790
MONEY_ARRAY = 791
NAME = 19
NAME_ARRAY = 1003
NUMERIC = 1700
NUMERIC_ARRAY = 1231
NULLTYPE = -1
OID = 26
POINT = 600
REAL = 700
REAL_ARRAY = 1021
SMALLINT = 21
SMALLINT_ARRAY = 1005
SMALLINT_VECTOR = 22
STRING = 1043
TEXT = 25
TEXT_ARRAY = 1009
TIME = 1083
TIMEDELTA = 1186
TIMESTAMP = 1114
TIMESTAMP_ARRAY = 1115
TIMESTAMPTZ = 1184
TIMESTAMPTZ_ARRAY = 1185
UNKNOWN = 705
UUID_TYPE = 2950
UUID_ARRAY = 2951
VARCHAR = 1043
VARCHAR_ARRAY = 1015
XID = 28


MIN_INT2, MAX_INT2 = -2 ** 15, 2 ** 15
MIN_INT4, MAX_INT4 = -2 ** 31, 2 ** 31
MIN_INT8, MAX_INT8 = -2 ** 63, 2 ** 63


def bool_in(data):
    return data == 't'


def bool_out(v):
    return 'true' if v else 'false'


def bytes_in(data):
    return bytes.fromhex(data[2:])


def bytes_out(v):
    return '\\x' + v.hex()


def cidr_out(v):
    return str(v)


def cidr_in(data):
    return ip_network(data, False) if '/' in data else ip_address(data)


def date_in(data):
    return Datetime.strptime(data, "%Y-%m-%d").date()


def date_out(v):
    return v.isoformat()


def enum_out(v):
    return str(v.value)


def float_out(v):
    return str(v)


def inet_in(data):
    return ip_network(data, False) if '/' in data else ip_address(data)


def inet_out(v):
    return str(v)


def int_in(data):
    return int(data)


def int_out(v):
    return str(v)


def json_in(data):
    return loads(data)


def json_out(v):
    return dumps(v)


def null_out(v):
    return None


def money_in(data):
    return data[1:]


def money_out(m):
    return str(m)


def numeric_in(data):
    return Decimal(data)


def numeric_out(d):
    return str(d)


def pginterval_in(data):
    return PGInterval.from_str(data)


def pginterval_out(v):
    return str(v)


def string_in(data):
    return data


def string_out(v):
    return v


def time_in(data):
    pattern = "%H:%M:%S.%f" if "." in data else "%H:%M:%S"
    return Datetime.strptime(data, pattern).time()


def time_out(v):
    return v.isoformat()


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
                f"Can't fit the interval {t} into a datetime.timedelta.")

    return Timedelta(**t)


def timedelta_out(v):
    return ' '.join(
        (
            str(v.days), "days", str(v.seconds), "seconds",
            str(v.microseconds), "microseconds"
        )
    )


def timestamp_in(data):
    if data in ('infinity', '-infinity'):
        return data

    pattern = "%Y-%m-%d %H:%M:%S.%f" if '.' in data else "%Y-%m-%d %H:%M:%S"
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


def unknown_out(v):
    return str(v)


def vector_in(data):
    return eval('[' + data.replace(' ', ',') + ']')


def uuid_out(v):
    return str(v)


def uuid_in(data):
    return UUID(data)


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


bool_array_in = _array_in(bool_in)
bytes_array_in = _array_in(bytes_in)
cidr_array_in = _array_in(cidr_in)
date_array_in = _array_in(date_in)
inet_array_in = _array_in(inet_in)
int_array_in = _array_in(int)
json_array_in = _array_in(json_in)
float_array_in = _array_in(float)
money_array_in = _array_in(money_in)
numeric_array_in = _array_in(numeric_in)
string_array_in = _array_in(string_in)
timestamp_array_in = _array_in(timestamp_in)
timestamptz_array_in = _array_in(timestamptz_in)
uuid_array_in = _array_in(uuid_in)


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


# pg element oid -> pg array typeoid
PG_ARRAY_TYPES = {
    BOOLEAN: BOOLEAN_ARRAY,          # bool[]
    BIGINT: BIGINT_ARRAY,            # int8[]
    BYTES: BYTES_ARRAY,              # bytea[]
    CIDR: CIDR_ARRAY,                # cidr[]
    DATE: DATE_ARRAY,                # date[]
    FLOAT: FLOAT_ARRAY,              # float8[]
    INET: INET_ARRAY,                # inet[]
    INTEGER: INTEGER_ARRAY,          # int4[]
    JSON: JSON_ARRAY,                # json[]
    JSONB: JSONB_ARRAY,              # jsonb[]
    MONEY: MONEY_ARRAY,              # money[]
    NUMERIC: NUMERIC_ARRAY,          # numeric[]
    SMALLINT: SMALLINT_ARRAY,        # int2[]
    TEXT: TEXT_ARRAY,                # text[]
    TIMESTAMP: TIMESTAMP_ARRAY,      # timestamp[]
    TIMESTAMPTZ: TIMESTAMPTZ_ARRAY,  # timestamptz[]
    UUID_TYPE: UUID_ARRAY,           # uuid[]
    VARCHAR: VARCHAR_ARRAY,          # varchar[]
    UNKNOWN: VARCHAR_ARRAY,          # any[]
}


def inspect_datetime(value):
    if value.tzinfo is None:
        return PY_TYPES[TIMESTAMP]
    else:
        return PY_TYPES[TIMESTAMPTZ]


min_int2, max_int2 = -2 ** 15, 2 ** 15
min_int4, max_int4 = -2 ** 31, 2 ** 31
min_int8, max_int8 = -2 ** 63, 2 ** 63


def inspect_int(value):
    if min_int2 < value < max_int2:
        return PY_TYPES[SMALLINT]
    if min_int4 < value < max_int4:
        return PY_TYPES[INTEGER]
    if min_int8 < value < max_int8:
        return PY_TYPES[BIGINT]
    return PY_TYPES[Decimal]


def array_find_first_element(arr):
    for v in array_flatten(arr):
        if v is not None:
            return v
    return None


def array_flatten(arr):
    for v in arr:
        if isinstance(v, list):
            for v2 in array_flatten(v):
                yield v2
        else:
            yield v


def array_inspect(array):
    first_element = None
    for v in array_flatten(array):
        if v is not None:
            first_element = v
            break

    if first_element is None:
        oid = VARCHAR
    else:
        oid, _ = make_param(PY_TYPES, first_element)

    try:
        array_oid = PG_ARRAY_TYPES[oid]
    except KeyError:
        raise InterfaceError(
            f"oid {oid} not supported as array contents")

    try:
        return PY_TYPES[array_oid]
    except KeyError:
        raise InterfaceError(f"array oid {array_oid} not supported")


def _make_array_out(ar, adapter_f):
    result = []
    for v in ar:

        if isinstance(v, list):
            val = _make_array_out(v, adapter_f)

        elif v is None:
            val = 'NULL'

        else:
            val = adapter_f(v)

        result.append(val)

    return '{' + ','.join(result) + '}'


def _array_out(adapter):
    def f(data):
        return _make_array_out(data, adapter)
    return f


bool_array_out = _array_out(bool_out)
date_array_out = _array_out(date_out)
float_array_out = _array_out(float_out)
inet_array_out = _array_out(inet_out)
int_array_out = _array_out(int_out)
numeric_array_out = _array_out(numeric_out)
money_array_out = _array_out(money_out)
timestamp_array_out = _array_out(timestamp_out)
timestamptz_array_out = _array_out(timestamptz_out)
uuid_array_out = _array_out(uuid_out)


def bytes_array_out(ar):
    result = []
    for v in ar:

        if isinstance(v, list):
            val = bytes_array_out(v)

        elif v is None:
            val = 'NULL'

        else:
            val = f'"\\{bytes_out(v)}"'

        result.append(val)

    return '{' + ','.join(result) + '}'


def json_array_out(ar):
    result = []
    for v in ar:

        if isinstance(v, list):
            val = json_array_out(v)

        elif v is None:
            val = 'NULL'

        else:
            val = array_string_escape(json_out(v))

        result.append(val)

    return '{' + ','.join(result) + '}'


def string_array_out(ar):
    result = []
    for v in ar:

        if isinstance(v, list):
            val = string_array_out(v)

        elif v is None:
            val = 'NULL'

        else:
            val = array_string_escape(v)

        result.append(val)

    return '{' + ','.join(result) + '}'


INSPECT_FUNCS = {
    Datetime: inspect_datetime,
    list: array_inspect,
    tuple: array_inspect,
    int: inspect_int
}


PY_TYPES = {
    BOOLEAN_ARRAY: (BOOLEAN_ARRAY, bool_array_out),        # bool[]
    BIGINT: (BIGINT, int_out),                             # int8
    BIGINT_ARRAY: (BIGINT_ARRAY, int_array_out),           # int8[]
    BYTES_ARRAY: (BYTES_ARRAY, bytes_array_out),           # bytes[]
    DATE_ARRAY: (DATE_ARRAY, date_array_out),              # date[]
    FLOAT_ARRAY: (FLOAT_ARRAY, float_array_out),           # float8[]
    INET_ARRAY: (INET_ARRAY, inet_array_out),              # inet[]
    INTEGER: (INTEGER, int_out),                           # int4
    INTEGER_ARRAY: (INTEGER_ARRAY, int_array_out),         # int4[]
    JSON_ARRAY: (JSON_ARRAY, json_array_out),              # json[]
    JSONB_ARRAY: (JSONB_ARRAY, json_array_out),            # jsonb[]
    MONEY: (MONEY, money_out),                             # money[]
    MONEY_ARRAY: (MONEY_ARRAY, numeric_array_out),         # money[]
    NUMERIC_ARRAY: (NUMERIC_ARRAY, numeric_array_out),     # numeric[]
    SMALLINT: (SMALLINT, int_out),                         # int2
    SMALLINT_ARRAY: (SMALLINT_ARRAY, int_array_out),       # int2[]
    TIMESTAMP: (TIMESTAMP, timestamp_out),                 # timestamp
    TIMESTAMP_ARRAY:
        (TIMESTAMP_ARRAY, timestamp_array_out),            # timestamp[]
    TIMESTAMPTZ: (TIMESTAMPTZ, timestamptz_out),           # timestamptz
    TIMESTAMPTZ_ARRAY:
        (TIMESTAMPTZ_ARRAY, timestamptz_array_out),        # timestamptz[]
    UUID_ARRAY: (UUID_ARRAY, uuid_array_out),              # uuid
    VARCHAR_ARRAY: (VARCHAR_ARRAY, string_array_out),      # varchar[]
    Date: (DATE, date_out),                                # date
    Decimal: (1700, numeric_out),                          # numeric
    Enum: (UNKNOWN, enum_out),                             # enum
    IPv4Address: (INET, inet_out),                         # inet
    IPv6Address: (INET, inet_out),                         # inet
    IPv4Network: (INET, inet_out),                         # inet
    IPv6Network: (INET, inet_out),                         # inet
    PGInterval: (1186, pginterval_out),                    # interval
    Time: (TIME, time_out),                                # time
    Timedelta: (1186, timedelta_out),                      # interval
    UUID: (UUID_TYPE, uuid_out),                           # uuid
    bool: (BOOLEAN, bool_out),                             # bool
    bytearray: (BYTES, bytes_out),                         # bytea
    dict: (JSONB, json_out),                               # jsonb
    float: (FLOAT, float_out),                             # float8
    type(None): (NULLTYPE, null_out),                      # null
    bytes: (BYTES, bytes_out),                             # bytea
    str: (UNKNOWN, string_out),                            # unknown
}


PG_TYPES = {
    BIGINT: int,                              # int8
    BIGINT_ARRAY: int_array_in,               # int8[]
    BOOLEAN: bool_in,                         # bool
    BOOLEAN_ARRAY: bool_array_in,             # bool[]
    BYTES: bytes_in,                          # bytea
    BYTES_ARRAY: bytes_array_in,              # bytea[]
    CHAR: string_in,                          # char
    CHAR_ARRAY: string_array_in,              # char[]
    CIDR_ARRAY: cidr_array_in,                # cidr[]
    CSTRING: string_in,                       # cstring
    CSTRING_ARRAY: string_array_in,           # cstring[]
    DATE: date_in,                            # date
    DATE_ARRAY: date_array_in,                # date[]
    FLOAT: float,                             # float8
    FLOAT_ARRAY: float_array_in,              # float8[]
    INET: inet_in,                            # inet
    INET_ARRAY: inet_array_in,                # inet[]
    INTEGER: int,                             # int4
    INTEGER_ARRAY: int_array_in,              # int4[]
    JSON: json_in,                            # json
    JSON_ARRAY: json_array_in,                # json[]
    JSONB: json_in,                           # jsonb
    JSONB_ARRAY: json_array_in,               # jsonb[]
    MACADDR: string_in,                       # MACADDR type
    MONEY: money_in,                          # money
    MONEY_ARRAY: money_array_in,              # money[]
    NAME: string_in,                          # name
    NAME_ARRAY: string_array_in,              # name[]
    NUMERIC: numeric_in,                      # numeric
    NUMERIC_ARRAY: numeric_array_in,          # numeric[]
    OID: int,                                 # oid
    REAL: float,                              # float4
    REAL_ARRAY: float_array_in,               # float4[]
    SMALLINT: int,                            # int2
    SMALLINT_ARRAY: int_array_in,             # int2[]
    SMALLINT_VECTOR: vector_in,               # int2vector
    TEXT: string_in,                          # text
    TEXT_ARRAY: string_array_in,              # text[]
    TIME: time_in,                            # time
    TIMEDELTA: timedelta_in,                  # interval
    TIMESTAMP: timestamp_in,                  # timestamp
    TIMESTAMP_ARRAY: timestamp_array_in,      # timestamp
    TIMESTAMPTZ: timestamptz_in,              # timestamptz
    TIMESTAMPTZ_ARRAY: timestamptz_array_in,  # timestamptz
    UNKNOWN: string_in,                       # unknown
    UUID_ARRAY: uuid_array_in,                # uuid[]
    UUID_TYPE: uuid_in,                       # uuid
    VARCHAR: string_in,                       # varchar
    VARCHAR_ARRAY: string_array_in,           # varchar[]
    XID: int,                                 # xid
}


# PostgreSQL encodings:
# https://www.postgresql.org/docs/current/multibyte.html
#
# Python encodings:
# https://docs.python.org/3/library/codecs.html
#
# Commented out encodings don't require a name change between PostgreSQL and
# Python.  If the py side is None, then the encoding isn't supported.
PG_PY_ENCODINGS = {
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


def make_param(py_types, value):
    typ = type(value)
    try:
        oid, func = py_types[typ]
    except KeyError:
        try:
            oid, func = INSPECT_FUNCS[typ](value)
        except KeyError:
            oid, func = None, None
            for k, v in py_types.items():
                try:
                    if isinstance(value, k):
                        oid, func = v
                        break
                except TypeError:
                    pass

            if oid is None:
                for k, v in INSPECT_FUNCS.items():
                    try:
                        if isinstance(value, k):
                            oid, func = v(value)
                            break
                    except TypeError:
                        pass
                    except KeyError:
                        pass

            if oid is None:
                oid, func = UNKNOWN, string_out

    return oid, func(value)


def make_params(py_types, values):
    oids, params = [], []
    for v in values:
        oid, param = make_param(py_types, v)
        oids.append(oid)
        params.append(param)

    return tuple(oids), tuple(params)
