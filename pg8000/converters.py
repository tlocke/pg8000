from datetime import (
    date as Date,
    datetime as Datetime,
    time as Time,
    timedelta as Timedelta,
    timezone as Timezone,
)
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address,
    IPv4Network,
    IPv6Address,
    IPv6Network,
    ip_address,
    ip_network,
)
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
FLOAT = 701
FLOAT_ARRAY = 1022
INET = 869
INET_ARRAY = 1041
INT2VECTOR = 22
INTEGER = 23
INTEGER_ARRAY = 1007
INTERVAL = 1186
INTERVAL_ARRAY = 1187
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
TIME_ARRAY = 1183
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


MIN_INT2, MAX_INT2 = -(2 ** 15), 2 ** 15
MIN_INT4, MAX_INT4 = -(2 ** 31), 2 ** 31
MIN_INT8, MAX_INT8 = -(2 ** 63), 2 ** 63


def bool_in(data):
    return data == "t"


def bool_out(v):
    return "true" if v else "false"


def bytes_in(data):
    return bytes.fromhex(data[2:])


def bytes_out(v):
    return "\\x" + v.hex()


def cidr_out(v):
    return str(v)


def cidr_in(data):
    return ip_network(data, False) if "/" in data else ip_address(data)


def date_in(data):
    return Datetime.strptime(data, "%Y-%m-%d").date()


def date_out(v):
    return v.isoformat()


def datetime_out(v):
    if v.tzinfo is None:
        return v.isoformat()
    else:
        return v.astimezone(Timezone.utc).isoformat()


def enum_out(v):
    return str(v.value)


def float_out(v):
    return str(v)


def inet_in(data):
    return ip_network(data, False) if "/" in data else ip_address(data)


def inet_out(v):
    return str(v)


def int_in(data):
    return int(data)


def int_out(v):
    return str(v)


def interval_in(data):
    t = {}

    curr_val = None
    for k in data.split():
        if ":" in k:
            t["hours"], t["minutes"], t["seconds"] = map(float, k.split(":"))
        else:
            try:
                curr_val = float(k)
            except ValueError:
                t[PGInterval.UNIT_MAP[k]] = curr_val

    for n in ["weeks", "months", "years", "decades", "centuries", "millennia"]:
        if n in t:
            raise InterfaceError(
                f"Can't fit the interval {t} into a datetime.timedelta."
            )

    return Timedelta(**t)


def interval_out(v):
    return f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds"


def json_in(data):
    return loads(data)


def json_out(v):
    return dumps(v)


def null_out(v):
    return None


def numeric_in(data):
    return Decimal(data)


def numeric_out(d):
    return str(d)


def pg_interval_in(data):
    return PGInterval.from_str(data)


def pg_interval_out(v):
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


def timestamp_in(data):
    if data in ("infinity", "-infinity"):
        return data

    pattern = "%Y-%m-%d %H:%M:%S.%f" if "." in data else "%Y-%m-%d %H:%M:%S"
    return Datetime.strptime(data, pattern)


def timestamptz_in(data):
    patt = "%Y-%m-%d %H:%M:%S.%f%z" if "." in data else "%Y-%m-%d %H:%M:%S%z"
    return Datetime.strptime(data + "00", patt)


def unknown_out(v):
    return str(v)


def vector_in(data):
    return eval("[" + data.replace(" ", ",") + "]")


def uuid_out(v):
    return str(v)


def uuid_in(data):
    return UUID(data)


class PGInterval:
    UNIT_MAP = {
        "year": "years",
        "years": "years",
        "millennia": "millennia",
        "millenium": "millennia",
        "centuries": "centuries",
        "century": "centuries",
        "decades": "decades",
        "decade": "decades",
        "years": "years",
        "year": "years",
        "months": "months",
        "month": "months",
        "mon": "months",
        "mons": "months",
        "weeks": "weeks",
        "week": "weeks",
        "days": "days",
        "day": "days",
        "hours": "hours",
        "hour": "hours",
        "minutes": "minutes",
        "minute": "minutes",
        "seconds": "seconds",
        "second": "seconds",
        "microseconds": "microseconds",
        "microsecond": "microseconds",
    }

    @staticmethod
    def from_str(interval_str):
        t = {}

        curr_val = None
        for k in interval_str.split():
            if ":" in k:
                hours_str, minutes_str, seconds_str = k.split(":")
                hours = int(hours_str)
                if hours != 0:
                    t["hours"] = hours
                minutes = int(minutes_str)
                if minutes != 0:
                    t["minutes"] = minutes
                try:
                    seconds = int(seconds_str)
                except ValueError:
                    seconds = float(seconds_str)

                if seconds != 0:
                    t["seconds"] = seconds

            else:
                try:
                    curr_val = int(k)
                except ValueError:
                    t[PGInterval.UNIT_MAP[k]] = curr_val

        return PGInterval(**t)

    def __init__(
        self,
        millennia=None,
        centuries=None,
        decades=None,
        years=None,
        months=None,
        weeks=None,
        days=None,
        hours=None,
        minutes=None,
        seconds=None,
        microseconds=None,
    ):
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
            ("millennia", self.millennia),
            ("centuries", self.centuries),
            ("decades", self.decades),
            ("years", self.years),
            ("months", self.months),
            ("weeks", self.weeks),
            ("days", self.days),
            ("hours", self.hours),
            ("minutes", self.minutes),
            ("seconds", self.seconds),
            ("microseconds", self.microseconds),
        ):
            if value is not None:
                res.append(str(value))
                res.append(name)

        return " ".join(res) + ">"

    def __str__(self):
        res = []
        if self.millennia is not None:
            res.append(str(self.millenia))
            res.append("millenia")

        if self.centuries is not None:
            res.append(str(self.centuries))
            res.append("centuries")

        if self.decades is not None:
            res.append(str(self.decades))
            res.append("decades")

        if self.years is not None:
            res.append(str(self.years))
            res.append("years")

        if self.months is not None:
            res.append(str(self.months))
            res.append("months")

        if self.weeks is not None:
            res.append(str(self.weeks))
            res.append("weeks")

        if self.days is not None:
            res.append(str(self.days))
            res.append("days")

        if self.hours is not None:
            res.append(str(self.hours))
            res.append("hours")

        if self.minutes is not None:
            res.append(str(self.minutes))
            res.append("minutes")

        if self.seconds is not None:
            res.append(str(self.seconds))
            res.append("seconds")

        if self.microseconds is not None:
            res.append(str(self.microseconds))
            res.append("microseconds")

        return " ".join(res)

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
            return s.months == o.months and s.days == o.days and s.seconds == o.seconds
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
            if c in ("}", ","):
                value = "".join(val)
                stack[-1].append(None if value == "NULL" else adapter(value))
                state = ArrayState.Out
            else:
                val.append(c)

        if state == ArrayState.Out:
            if c == "{":
                a = []
                stack[-1].append(a)
                stack.append(a)
            elif c == "}":
                stack.pop()
            elif c == ",":
                pass
            elif c == '"':
                val = []
                state = ArrayState.InString
            else:
                val = [c]
                state = ArrayState.InValue

        elif state == ArrayState.InString:
            if c == '"':
                stack[-1].append(adapter("".join(val)))
                state = ArrayState.Out
            elif c == "\\":
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
interval_array_in = _array_in(interval_in)
json_array_in = _array_in(json_in)
float_array_in = _array_in(float)
numeric_array_in = _array_in(numeric_in)
string_array_in = _array_in(string_in)
time_array_in = _array_in(time_in)
timestamp_array_in = _array_in(timestamp_in)
timestamptz_array_in = _array_in(timestamptz_in)
uuid_array_in = _array_in(uuid_in)


def array_string_escape(v):
    cs = []
    for c in v:
        if c == "\\":
            cs.append("\\")
        elif c == '"':
            cs.append("\\")
        cs.append(c)
    val = "".join(cs)
    if (
        len(val) == 0
        or val == "NULL"
        or any([c in val for c in ("{", "}", ",", " ", "\\")])
    ):
        val = f'"{val}"'
    return val


def array_out(ar):
    result = []
    for v in ar:

        if isinstance(v, (list, tuple)):
            val = array_out(v)

        elif v is None:
            val = "NULL"

        elif isinstance(v, dict):
            val = array_string_escape(json_out(v))

        elif isinstance(v, (bytes, bytearray)):
            val = f'"\\{bytes_out(v)}"'

        elif isinstance(v, str):
            val = array_string_escape(v)

        else:
            val = make_param(PY_TYPES, v)

        result.append(val)

    return "{" + ",".join(result) + "}"


PY_PG = {
    Date: DATE,
    Decimal: NUMERIC,
    IPv4Address: INET,
    IPv6Address: INET,
    IPv4Network: INET,
    IPv6Network: INET,
    PGInterval: INTERVAL,
    Time: TIME,
    Timedelta: INTERVAL,
    UUID: UUID_TYPE,
    bool: BOOLEAN,
    bytearray: BYTES,
    dict: JSONB,
    float: FLOAT,
    type(None): NULLTYPE,
    bytes: BYTES,
    str: TEXT,
}


PY_TYPES = {
    Date: date_out,  # date
    Datetime: datetime_out,
    Decimal: numeric_out,  # numeric
    Enum: enum_out,  # enum
    IPv4Address: inet_out,  # inet
    IPv6Address: inet_out,  # inet
    IPv4Network: inet_out,  # inet
    IPv6Network: inet_out,  # inet
    PGInterval: interval_out,  # interval
    Time: time_out,  # time
    Timedelta: interval_out,  # interval
    UUID: uuid_out,  # uuid
    bool: bool_out,  # bool
    bytearray: bytes_out,  # bytea
    dict: json_out,  # jsonb
    float: float_out,  # float8
    type(None): null_out,  # null
    bytes: bytes_out,  # bytea
    str: string_out,  # unknown
    int: int_out,
    list: array_out,
    tuple: array_out,
}


PG_TYPES = {
    BIGINT: int,  # int8
    BIGINT_ARRAY: int_array_in,  # int8[]
    BOOLEAN: bool_in,  # bool
    BOOLEAN_ARRAY: bool_array_in,  # bool[]
    BYTES: bytes_in,  # bytea
    BYTES_ARRAY: bytes_array_in,  # bytea[]
    CHAR: string_in,  # char
    CHAR_ARRAY: string_array_in,  # char[]
    CIDR_ARRAY: cidr_array_in,  # cidr[]
    CSTRING: string_in,  # cstring
    CSTRING_ARRAY: string_array_in,  # cstring[]
    DATE: date_in,  # date
    DATE_ARRAY: date_array_in,  # date[]
    FLOAT: float,  # float8
    FLOAT_ARRAY: float_array_in,  # float8[]
    INET: inet_in,  # inet
    INET_ARRAY: inet_array_in,  # inet[]
    INTEGER: int,  # int4
    INTEGER_ARRAY: int_array_in,  # int4[]
    JSON: json_in,  # json
    JSON_ARRAY: json_array_in,  # json[]
    JSONB: json_in,  # jsonb
    JSONB_ARRAY: json_array_in,  # jsonb[]
    MACADDR: string_in,  # MACADDR type
    MONEY: string_in,  # money
    MONEY_ARRAY: string_array_in,  # money[]
    NAME: string_in,  # name
    NAME_ARRAY: string_array_in,  # name[]
    NUMERIC: numeric_in,  # numeric
    NUMERIC_ARRAY: numeric_array_in,  # numeric[]
    OID: int,  # oid
    INTERVAL: interval_in,  # interval
    INTERVAL_ARRAY: interval_array_in,  # interval[]
    REAL: float,  # float4
    REAL_ARRAY: float_array_in,  # float4[]
    SMALLINT: int,  # int2
    SMALLINT_ARRAY: int_array_in,  # int2[]
    SMALLINT_VECTOR: vector_in,  # int2vector
    TEXT: string_in,  # text
    TEXT_ARRAY: string_array_in,  # text[]
    TIME: time_in,  # time
    TIME_ARRAY: time_array_in,  # time[]
    INTERVAL: interval_in,  # interval
    TIMESTAMP: timestamp_in,  # timestamp
    TIMESTAMP_ARRAY: timestamp_array_in,  # timestamp
    TIMESTAMPTZ: timestamptz_in,  # timestamptz
    TIMESTAMPTZ_ARRAY: timestamptz_array_in,  # timestamptz
    UNKNOWN: string_in,  # unknown
    UUID_ARRAY: uuid_array_in,  # uuid[]
    UUID_TYPE: uuid_in,  # uuid
    VARCHAR: string_in,  # varchar
    VARCHAR_ARRAY: string_array_in,  # varchar[]
    XID: int,  # xid
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
    try:
        func = py_types[type(value)]
    except KeyError:
        func = str
        for k, v in py_types.items():
            try:
                if isinstance(value, k):
                    func = v
                    break
            except TypeError:
                pass

    return func(value)


def make_params(py_types, values):
    return tuple([make_param(py_types, v) for v in values])
