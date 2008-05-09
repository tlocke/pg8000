# vim: sw=4:expandtab:foldmethod=marker
#
# Copyright (c) 2007, Mathieu Fenniak
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"

import datetime
import decimal
import struct
from errors import NotSupportedError

class Bytea(str):
    pass

class Interval(object):
    def __init__(self, microseconds, days, months):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def __repr__(self):
        return "<Interval %s months %s days %s microseconds>" % (self.months, self.days, self.microseconds)

    def __cmp__(self, other):
        c = cmp(self.months, other.months)
        if c != 0: return c
        c = cmp(self.days, other.days)
        if c != 0: return c
        return cmp(self.microseconds, other.microseconds)

def pg_type_info(typ):
    data = py_types.get(typ)
    if data == None:
        raise NotSupportedError("type %r not mapped to pg type" % typ)
    type_oid = data.get("tid")
    if type_oid == None:
        raise InternalError("type %r has no type_oid" % typ)
    elif type_oid == -1:
        # special case: NULL values
        return type_oid, 0
    # prefer bin, but go with whatever exists
    if data.get("bin_out"):
        format = 1
    elif data.get("txt_out"):
        format = 0
    else:
        raise InternalError("no conversion fuction for type %r" % typ)
    return type_oid, format

def pg_value(v, fc, **kwargs):
    typ = type(v)
    data = py_types.get(typ)
    if data == None:
        raise NotSupportedError("type %r not mapped to pg type" % typ)
    elif data.get("tid") == -1:
        # special case: NULL values
        return None
    if fc == 0:
        func = data.get("txt_out")
    elif fc == 1:
        func = data.get("bin_out")
    else:
        raise InternalError("unrecognized format code %r" % fc)
    if func == None:
        raise NotSupportedError("type %r, format code %r not supported" % (typ, fc))
    return func(v, **kwargs)

def py_type_info(description):
    type_oid = description['type_oid']
    data = pg_types.get(type_oid)
    if data == None:
        raise NotSupportedError("type oid %r not mapped to py type" % type_oid)
    # prefer bin, but go with whatever exists
    if data.get("bin_in"):
        format = 1
    elif data.get("txt_in"):
        format = 0
    else:
        raise InternalError("no conversion fuction for type oid %r" % type_oid)
    return format

def py_value(v, description, **kwargs):
    if v == None:
        # special case - NULL value
        return None
    type_oid = description['type_oid']
    format = description['format']
    data = pg_types.get(type_oid)
    if data == None:
        raise NotSupportedError("type oid %r not supported" % type_oid)
    if format == 0:
        func = data.get("txt_in")
    elif format == 1:
        func = data.get("bin_in")
    else:
        raise NotSupportedError("format code %r not supported" % format)
    if func == None:
        raise NotSupportedError("data response format %r, type %r not supported" % (format, type_oid))
    return func(v, **kwargs)

def boolrecv(data, **kwargs):
    return data == "\x01"

def boolout(v, **kwargs):
    if v:
        return 't'
    else:
        return 'f'

def int2recv(data, **kwargs):
    return struct.unpack("!h", data)[0]

def int4recv(data, **kwargs):
    return struct.unpack("!i", data)[0]

def int8recv(data, **kwargs):
    return struct.unpack("!q", data)[0]

def float4recv(data, **kwargs):
    return struct.unpack("!f", data)[0]

def float8recv(data, **kwargs):
    return struct.unpack("!d", data)[0]

def float8send(v, **kwargs):
    return struct.pack("!d", v)

def timestamp_recv(data, integer_datetimes, **kwargs):
    if integer_datetimes:
        # data is 64-bit integer representing milliseconds since 2000-01-01
        val = struct.unpack("!q", data)[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds = val)
    else:
        # data is double-precision float representing seconds since 2000-01-01
        val = struct.unpack("!d", data)[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds = val)

def timestamp_send(v, integer_datetimes, **kwargs):
    delta = v - datetime.datetime(2000, 1, 1)
    val = delta.microseconds + (delta.seconds * 1000000) + (delta.days * 86400000000)
    if integer_datetimes:
        return struct.pack("!q", val)
    else:
        return struct.pack("!d", val / 1000.0 / 1000.0)

def date_in(data, **kwargs):
    year = int(data[0:4])
    month = int(data[5:7])
    day = int(data[8:10])
    return datetime.date(year, month, day)

def date_out(v, **kwargs):
    return v.isoformat()

def time_in(data, **kwargs):
    hour = int(data[0:2])
    minute = int(data[3:5])
    sec = decimal.Decimal(data[6:])
    return datetime.time(hour, minute, int(sec), int((sec - int(sec)) * 1000000))

def time_out(v, **kwargs):
    return v.isoformat()

def numeric_in(data, **kwargs):
    if data.find(".") == -1:
        return int(data)
    else:
        return decimal.Decimal(data)

def numeric_out(v, **kwargs):
    return str(v)

def encoding_convert(encoding):
    encodings = {"sql_ascii": "ascii"}
    return encodings.get(encoding.lower(), encoding)

def varcharin(data, client_encoding, **kwargs):
    return unicode(data, encoding_convert(client_encoding))

def textout(v, client_encoding, **kwargs):
    return v.encode(encoding_convert(client_encoding))

def timestamptz_in(data, **kwargs):
    year = int(data[0:4])
    month = int(data[5:7])
    day = int(data[8:10])
    hour = int(data[11:13])
    minute = int(data[14:16])
    tz_sep = data.rfind("-")
    sec = decimal.Decimal(data[17:tz_sep])
    tz = data[tz_sep:]
    return datetime.datetime(year, month, day, hour, minute, int(sec), int((sec - int(sec)) * 1000000), FixedOffsetTz(tz))

class FixedOffsetTz(datetime.tzinfo):
    def __init__(self, hrs):
        self.hrs = int(hrs)
        self.name = hrs

    def utcoffset(self, dt):
        return datetime.timedelta(hours=1) * self.hrs

    def tzname(self, dt):
        return self.name

    def dst(self, dt):
        return datetime.timedelta(0)

    def __eq__(self, other):
        if not isinstance(other, FixedOffsetTz):
            return False
        return self.hrs == other.hrs

def byteasend(v, **kwargs):
    return str(v)

def bytearecv(data, **kwargs):
    return Bytea(data)

# interval support does not provide a Python-usable interval object yet
def interval_recv(data, integer_datetimes, **kwargs):
    if integer_datetimes:
        microseconds, days, months = struct.unpack("!qii", data)
    else:
        seconds, days, months = struct.unpack("!dii", data)
        microseconds = int(seconds * 1000 * 1000)
    return Interval(microseconds, days, months)

def interval_send(data, integer_datetimes, **kwargs):
    if integer_datetimes:
        return struct.pack("!qii", data.microseconds, data.days, data.months)
    else:
        return struct.pack("!dii", data.microseconds / 1000.0 / 1000.0, data.days, data.months)

py_types = {
    bool: {"tid": 16, "txt_out": boolout},
    int: {"tid": 1700, "txt_out": numeric_out},
    long: {"tid": 1700, "txt_out": numeric_out},
    str: {"tid": 25, "txt_out": textout},
    unicode: {"tid": 25, "txt_out": textout},
    float: {"tid": 701, "bin_out": float8send},
    decimal.Decimal: {"tid": 1700, "txt_out": numeric_out},
    Bytea: {"tid": 17, "bin_out": byteasend},
    datetime.datetime: {"tid": 1114, "bin_out": timestamp_send},
    datetime.date: {"tid": 1082, "txt_out": date_out},
    datetime.time: {"tid": 1083, "txt_out": time_out},
    Interval: {"tid": 1186, "bin_out": interval_send},
    type(None): {"tid": -1},
}

pg_types = {
    16: {"bin_in": boolrecv},
    17: {"bin_in": bytearecv},
    19: {"txt_in": varcharin}, # name type
    20: {"bin_in": int8recv},
    21: {"bin_in": int2recv},
    23: {"bin_in": int4recv},
    25: {"txt_in": varcharin}, # TEXT type
    26: {"txt_in": numeric_in}, # oid type
    700: {"bin_in": float4recv},
    701: {"bin_in": float8recv},
    1042: {"txt_in": varcharin}, # CHAR type
    1043: {"txt_in": varcharin}, # VARCHAR type
    1082: {"txt_in": date_in},
    1083: {"txt_in": time_in},
    1114: {"bin_in": timestamp_recv},
    1184: {"txt_in": timestamptz_in}, # timestamp w/ tz
    1186: {"bin_in": interval_recv},
    1700: {"txt_in": numeric_in},
    2275: {"txt_in": varcharin}, # cstring
}



