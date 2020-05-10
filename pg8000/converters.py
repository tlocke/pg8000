from decimal import Decimal
from json import loads, dumps
from datetime import (
    datetime as Datetime, date as Date, time as Time, timedelta as Timedelta,
    timezone as Timezone)
from ipaddress import ip_address, ip_network
from uuid import UUID
from time import localtime
from pg8000.exceptions import InterfaceError
from enum import Enum


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


class PGType():
    def __init__(self, value):
        self.value = value

    def encode(self, encoding):
        return str(self.value).encode(encoding)


class PGEnum(PGType):
    def __init__(self, value):
        if isinstance(value, str):
            self.value = value
        else:
            self.value = value.value


class PGJson(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGJsonb(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGTsvector(PGType):
    pass


class PGVarchar(str):
    pass


class PGText(str):
    pass


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
