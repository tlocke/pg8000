from collections import defaultdict

from pg8000.converters import (
    BIGINT,
    BOOLEAN,
    BOOLEAN_ARRAY,
    BYTES,
    CHAR,
    CHAR_ARRAY,
    DATE,
    FLOAT,
    FLOAT_ARRAY,
    INET,
    INT2VECTOR,
    INTEGER,
    INTEGER_ARRAY,
    INTERVAL,
    JSON,
    JSONB,
    MACADDR,
    NAME,
    NAME_ARRAY,
    NULLTYPE,
    NUMERIC,
    NUMERIC_ARRAY,
    OID,
    PGInterval,
    STRING,
    TEXT,
    TEXT_ARRAY,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    UNKNOWN,
    UUID_TYPE,
    VARCHAR,
    VARCHAR_ARRAY,
    XID,
    identifier,
    literal,
    make_params,
)
from pg8000.core import CoreConnection, ver
from pg8000.exceptions import DatabaseError, Error, InterfaceError
from pg8000.types import Range

__version__ = ver

# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
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


OUTSIDE = 0  # outside quoted string
INSIDE_SQ = 1  # inside single-quote string '...'
INSIDE_QI = 2  # inside quoted identifier   "..."
INSIDE_ES = 3  # inside escaped single-quote string, E'...'
INSIDE_PN = 4  # inside parameter name eg. :name
INSIDE_CO = 5  # inside inline comment eg. --


def to_statement(query):
    in_quote_escape = False
    placeholders = []
    output_query = []
    state = OUTSIDE
    prev_c = None
    for i, c in enumerate(query):
        if i + 1 < len(query):
            next_c = query[i + 1]
        else:
            next_c = None

        if state == OUTSIDE:
            if c == "'":
                output_query.append(c)
                if prev_c == "E":
                    state = INSIDE_ES
                else:
                    state = INSIDE_SQ
            elif c == '"':
                output_query.append(c)
                state = INSIDE_QI
            elif c == "-":
                output_query.append(c)
                if prev_c == "-":
                    state = INSIDE_CO
            elif c == ":" and next_c not in ":=" and prev_c != ":":
                state = INSIDE_PN
                placeholders.append("")
            else:
                output_query.append(c)

        elif state == INSIDE_SQ:
            if c == "'":
                if in_quote_escape:
                    in_quote_escape = False
                elif next_c == "'":
                    in_quote_escape = True
                else:
                    state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_QI:
            if c == '"':
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_ES:
            if c == "'" and prev_c != "\\":
                # check for escaped single-quote
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_PN:
            placeholders[-1] += c
            if next_c is None or (not next_c.isalnum() and next_c != "_"):
                state = OUTSIDE
                try:
                    pidx = placeholders.index(placeholders[-1], 0, -1)
                    output_query.append(f"${pidx + 1}")
                    del placeholders[-1]
                except ValueError:
                    output_query.append(f"${len(placeholders)}")

        elif state == INSIDE_CO:
            output_query.append(c)
            if c == "\n":
                state = OUTSIDE

        prev_c = c

    for reserved in ("types", "stream"):
        if reserved in placeholders:
            raise InterfaceError(
                f"The name '{reserved}' can't be used as a placeholder because it's "
                f"used for another purpose."
            )

    def make_vals(args):
        vals = []
        for p in placeholders:
            try:
                vals.append(args[p])
            except KeyError:
                raise InterfaceError(
                    f"There's a placeholder '{p}' in the query, but no matching "
                    f"keyword argument."
                )
        return tuple(vals)

    return "".join(output_query), make_vals


class Connection(CoreConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._context = None

    @property
    def columns(self):
        context = self._context
        if context is None:
            return None
        return context.columns

    @property
    def row_count(self):
        context = self._context
        if context is None:
            return None
        return context.row_count

    def run(self, sql, stream=None, types=None, **params):
        if len(params) == 0 and stream is None:
            self._context = self.execute_simple(sql)
        else:
            statement, make_vals = to_statement(sql)
            oids = () if types is None else make_vals(defaultdict(lambda: None, types))
            self._context = self.execute_unnamed(
                statement, make_vals(params), oids=oids, stream=stream
            )
        return self._context.rows

    def prepare(self, sql):
        return PreparedStatement(self, sql)


class PreparedStatement:
    def __init__(self, con, sql, types=None):
        self.con = con
        self.statement, self.make_vals = to_statement(sql)
        oids = () if types is None else self.make_vals(defaultdict(lambda: None, types))
        self.name_bin, self.cols, self.input_funcs = con.prepare_statement(
            self.statement, oids
        )

    @property
    def columns(self):
        return self._context.columns

    def run(self, stream=None, **params):
        params = make_params(self.con.py_types, self.make_vals(params))

        self._context = self.con.execute_named(
            self.name_bin, params, self.cols, self.input_funcs, self.statement
        )

        return self._context.rows

    def close(self):
        self.con.close_prepared_statement(self.name_bin)


__all__ = [
    "BIGINT",
    "BOOLEAN",
    "BOOLEAN_ARRAY",
    "BYTES",
    "CHAR",
    "CHAR_ARRAY",
    "DATE",
    "DatabaseError",
    "Error",
    "FLOAT",
    "FLOAT_ARRAY",
    "INET",
    "INT2VECTOR",
    "INTEGER",
    "INTEGER_ARRAY",
    "INTERVAL",
    "InterfaceError",
    "JSON",
    "JSONB",
    "MACADDR",
    "NAME",
    "NAME_ARRAY",
    "NULLTYPE",
    "NUMERIC",
    "NUMERIC_ARRAY",
    "OID",
    "PGInterval",
    "Range",
    "STRING",
    "TEXT",
    "TEXT_ARRAY",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "UNKNOWN",
    "UUID_TYPE",
    "VARCHAR",
    "VARCHAR_ARRAY",
    "XID",
    "identifier",
    "literal",
]
