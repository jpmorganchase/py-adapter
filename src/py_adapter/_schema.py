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

import avro.constants
import avro.schema

_DEBUG_VALIDATE = False
_debug_validate_indent = 0

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
    global _DEBUG_VALIDATE
    global _debug_validate_indent
    expected_type = expected_schema.type
    name = getattr(expected_schema, "name", "")
    if name:
        name = " " + name
    if expected_type in ("array", "map", "union", "record"):
        if _DEBUG_VALIDATE:
            print(
                "{!s}{!s}{!s}: {!s} {{".format(
                    " " * _debug_validate_indent, expected_schema.type, name, type(datum).__name__
                ),
                file=sys.stderr,
            )
            _debug_validate_indent += 2
            if datum is not None and not datum:
                print("{!s}<Empty>".format(" " * _debug_validate_indent), file=sys.stderr)
        result = _match[expected_type](expected_schema, datum)
        if _DEBUG_VALIDATE:
            _debug_validate_indent -= 2
            print("{!s}}} -> {!s}".format(" " * _debug_validate_indent, result), file=sys.stderr)
    else:
        result = _match[expected_type](expected_schema, datum)
        if _DEBUG_VALIDATE:
            print(
                "{!s}{!s}{!s}: {!s} -> {!s}".format(
                    " " * _debug_validate_indent, expected_schema.type, name, type(datum).__name__, result
                ),
                file=sys.stderr,
            )
    return result


def _is_timezone_aware_datetime(dt: datetime.datetime) -> bool:
    """Return whether a given datetime object contains a timezone"""
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def _match_record(schema: avro.schema.RecordSchema, datum: Any) -> float:
    """
    Return the degree to which given data matches a schema

    This return a number between 0 (no match/incompatible data) and 1 (all fields are provided). Partial matches simply
    mean some fields are provided.
    """
    if not isinstance(datum, dict):
        return 0.0

    # TODO: the original validation function also recursively validated the data for each field. Need to decide whether
    # that is necessary here or not.
    provided_fields = datum.keys()
    schema_fields = {f.name for f in schema.fields}
    # This is a rather arbitrary scheme for determining "best" match. It does not consider optional vs required fields.
    # It needs to penalize extra fields certainly.
    return len(provided_fields & schema_fields) / len(provided_fields | schema_fields)


_match = {
    "null": lambda schema, datum: float(datum is None),
    "boolean": lambda schema, datum: float(isinstance(datum, bool)),
    "string": lambda schema, datum: float(
        isinstance(datum, str) or (isinstance(datum, (dict, list)) and schema.get_prop("logicalType") == "json")
    ),
    "bytes": lambda schema, datum: float(
        (isinstance(datum, bytes))
        or (isinstance(datum, Decimal) and getattr(schema, "logical_type", None) == avro.constants.DECIMAL)
    ),
    "int": lambda schema, datum: float(
        ((isinstance(datum, int)))
        # Modified here as we serialize dates as timestamps in milliseconds
        and (LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE)
        or (isinstance(datum, datetime.date) and getattr(schema, "logical_type", None) == avro.constants.DATE)
        or (isinstance(datum, datetime.time) and getattr(schema, "logical_type", None) == avro.constants.TIME_MILLIS)
    ),
    "long": lambda schema, datum: float(
        (isinstance(datum, int))
        and (LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE)
        or (isinstance(datum, datetime.time) and getattr(schema, "logical_type", None) == avro.constants.TIME_MICROS)
        or (
            (isinstance(datum, datetime.datetime) and _is_timezone_aware_datetime(datum) or isinstance(datum, str))
            and getattr(schema, "logical_type", None)
            in (avro.constants.TIMESTAMP_MILLIS, avro.constants.TIMESTAMP_MICROS)
        )
    ),
    "float": lambda schema, datum: float(isinstance(datum, (int, float))),
    "fixed": lambda schema, datum: float(
        (isinstance(datum, bytes) and len(datum) == schema.size)
        or (isinstance(datum, Decimal) and getattr(schema, "logical_type", None) == avro.constants.DECIMAL)
    ),
    "enum": lambda schema, datum: float(datum in schema.symbols),
    "array": lambda schema, datum: float(isinstance(datum, list) and all(match(schema.items, item) for item in datum)),
    "map": lambda schema, datum: float(
        isinstance(datum, dict)
        and all(isinstance(key, str) for key in datum)
        and all(match(schema.values, value) for value in datum.values())
    ),
    "union": lambda schema, datum: float(any(match(branch, datum) for branch in schema.schemas)),
    "record": _match_record,
}
_match["double"] = _match["float"]
_match["error_union"] = _match["union"]
_match["error"] = _match["request"] = _match["record"]
