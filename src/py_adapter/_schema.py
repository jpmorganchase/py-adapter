# Copyright 2023 J.P. Morgan Chase & Co.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""
This schema validation logic adapted from avro.io, (c) The Apache Software Foundation, licenced under the Apache License
Version 2.0.

The logic is used only to pick the best matching schema from a union.
"""

import datetime
import sys
from decimal import Decimal
from struct import Struct
from typing import Any

import avro.schema
from avro import constants

_DEBUG_VALIDATE_INDENT = 0
_DEBUG_VALIDATE = False

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1

STRUCT_FLOAT = Struct("<f")  # big-endian float
STRUCT_DOUBLE = Struct("<d")  # big-endian double
STRUCT_SIGNED_SHORT = Struct(">h")  # big-endian signed short
STRUCT_SIGNED_INT = Struct(">i")  # big-endian signed int
STRUCT_SIGNED_LONG = Struct(">q")  # big-endian signed long


def match(expected_schema: avro.schema.Schema, datum: Any) -> float:
    """
    Determines to what extent some data is an instance of a schema as a number between 0 (not at all) and 1
    (perfect fit).

    Values between 0 and 1 are used for record schemas where certain fields might be omitted from the data or where
    extra fields are provided.
    """
    global _DEBUG_VALIDATE_INDENT
    global _DEBUG_VALIDATE
    expected_type = expected_schema.type
    name = getattr(expected_schema, "name", "")
    if name:
        name = " " + name
    if expected_type in ("array", "map", "union", "record"):
        if _DEBUG_VALIDATE:
            print(
                "{!s}{!s}{!s}: {!s} {{".format(
                    " " * _DEBUG_VALIDATE_INDENT, expected_schema.type, name, type(datum).__name__
                ),
                file=sys.stderr,
            )
            _DEBUG_VALIDATE_INDENT += 2
            if datum is not None and not datum:
                print("{!s}<Empty>".format(" " * _DEBUG_VALIDATE_INDENT), file=sys.stderr)
        result = _match[expected_type](expected_schema, datum)
        if _DEBUG_VALIDATE:
            _DEBUG_VALIDATE_INDENT -= 2
            print("{!s}}} -> {!s}".format(" " * _DEBUG_VALIDATE_INDENT, result), file=sys.stderr)
    else:
        result = _match[expected_type](expected_schema, datum)
        if _DEBUG_VALIDATE:
            print(
                "{!s}{!s}{!s}: {!s} -> {!s}".format(
                    " " * _DEBUG_VALIDATE_INDENT, expected_schema.type, name, type(datum).__name__, result
                ),
                file=sys.stderr,
            )
    return result


def _is_timezone_aware_datetime(dt) -> bool:
    """Return whether a given datetime object contains a timezone"""
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def _match_record(s: avro.schema.RecordSchema, d: Any) -> float:
    """
    Return the degree to which given data matches a schema

    This return a number between 0 (no match/incompatible data) and 1 (all fields are provided). Partial matches simply
    mean some fields are provided.
    """
    if not isinstance(d, dict):
        return 0.0

    # TODO: the original validation function also recursively validated the data for each field. Need to decide whether
    # that is necessary here or not.
    provided_fields = d.keys()
    schema_fields = {f.name for f in s.fields}
    # This is a rather arbitrary scheme for determining "best" match. It does not consider optional vs required fields.
    # It needs to penalize extra fields certainly.
    return len(provided_fields & schema_fields) / len(provided_fields | schema_fields)


_match = {
    "null": lambda s, d: float(d is None),
    "boolean": lambda s, d: float(isinstance(d, bool)),
    "string": lambda s, d: float(
        isinstance(d, str) or (isinstance(d, (dict, list)) and s.get_prop("logicalType") == "json")
    ),
    "bytes": lambda s, d: float(
        (isinstance(d, bytes)) or (isinstance(d, Decimal) and getattr(s, "logical_type", None) == constants.DECIMAL)
    ),
    "int": lambda s, d: float(
        ((isinstance(d, int)))
        # Modified here as we serialize dates as timestamps in milliseconds
        and (LONG_MIN_VALUE <= d <= LONG_MAX_VALUE)
        or (isinstance(d, datetime.date) and getattr(s, "logical_type", None) == constants.DATE)
        or (isinstance(d, datetime.time) and getattr(s, "logical_type", None) == constants.TIME_MILLIS)
    ),
    "long": lambda s, d: float(
        (isinstance(d, int))
        and (LONG_MIN_VALUE <= d <= LONG_MAX_VALUE)
        or (isinstance(d, datetime.time) and getattr(s, "logical_type", None) == constants.TIME_MICROS)
        or (
            (isinstance(d, datetime.date) and _is_timezone_aware_datetime(d) or isinstance(d, str))
            and getattr(s, "logical_type", None) in (constants.TIMESTAMP_MILLIS, constants.TIMESTAMP_MICROS)
        )
    ),
    "float": lambda s, d: float(isinstance(d, (int, float))),
    "fixed": lambda s, d: float(
        (isinstance(d, bytes) and len(d) == s.size)
        or (isinstance(d, Decimal) and getattr(s, "logical_type", None) == constants.DECIMAL)
    ),
    "enum": lambda s, d: float(d in s.symbols),
    "array": lambda s, d: float(isinstance(d, list) and all(match(s.items, item) for item in d)),
    "map": lambda s, d: float(
        isinstance(d, dict)
        and all(isinstance(key, str) for key in d)
        and all(match(s.values, value) for value in d.values())
    ),
    "union": lambda s, d: float(any(match(branch, d) for branch in s.schemas)),
    "record": _match_record,
}
_match["double"] = _match["float"]
_match["error_union"] = _match["union"]
_match["error"] = _match["request"] = _match["record"]
