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
Avro serializer/deserializer **py-adapter** plugin
"""

import functools
from collections.abc import Iterable, Iterator
from typing import BinaryIO, Type

import fastavro.types
import orjson

import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, stream: BinaryIO, py_type: Type, writer_schema: bytes) -> BinaryIO:
    """
    Serialize an object of basic Python types as Avro bytes

    :param obj:           Python object to serialize
    :param stream:        File-like object to serialize data to
    :param py_type:       Original Python class associated with the basic object
    :param writer_schema: Avro schema to serialize the data with, as JSON bytes.
    """
    import fastavro.write

    writer_schema = writer_schema or _default_schema(py_type)
    schema_obj = _parse_fastavro_schema(writer_schema)
    # TODO: add support for writer which embeds the schema
    fastavro.write.schemaless_writer(stream, schema=schema_obj, record=obj)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def serialize_many(objs: Iterable[py_adapter.Basic], stream: BinaryIO, py_type: Type, writer_schema: bytes) -> BinaryIO:
    """
    Serialize multiple Python objects of basic types as Avro container file format.

    :param objs:          Python objects to serialize
    :param stream:        File-like object to serialize data to
    :param py_type:       Original Python class associated with the basic object
    :param writer_schema: Avro schema to serialize the data with, as JSON bytes.
    """
    import fastavro.write

    writer_schema = writer_schema or _default_schema(py_type)
    schema_obj = _parse_fastavro_schema(writer_schema)
    fastavro.write.writer(stream, schema=schema_obj, records=objs)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def deserialize(stream: BinaryIO, py_type: Type, writer_schema: bytes, reader_schema: bytes) -> py_adapter.Basic:
    """
    Deserialize Avro bytes as an object of basic Python types

    :param stream:        File-like object to deserialize
    :param py_type:       Python class the basic object will ultimately be deserialized into
    :param writer_schema: Avro schema used to serialize the data with, as JSON bytes.
    :param reader_schema: Avro schema to deserialize the data with, as JSON bytes. The reader schema should be
                          compatible with the writer schema.
    """
    import fastavro.read

    writer_schema = writer_schema or _default_schema(py_type)
    writer_schema_obj = _parse_fastavro_schema(writer_schema)
    reader_schema_obj = _parse_fastavro_schema(reader_schema) if reader_schema else None
    # TODO: add support for reader of data with embedded (writer) schema
    basic_obj = fastavro.read.schemaless_reader(
        stream, writer_schema=writer_schema_obj, reader_schema=reader_schema_obj
    )
    return basic_obj


@py_adapter.plugin.hook
def deserialize_many(
    stream: BinaryIO, py_type: Type, writer_schema: bytes, reader_schema: bytes
) -> Iterator[py_adapter.Basic]:
    """
    Deserialize Avro container file format data as an iterator over objects of basic Python types

    :param stream:        File-like object to deserialize
    :param py_type:       Python class the basic object will ultimately be deserialized into
    :param writer_schema: Avro schema used to serialize the data with, as JSON bytes.
    :param reader_schema: Avro schema to deserialize the data with, as JSON bytes. The reader schema should be
                          compatible with the writer schema.
    """
    import fastavro.read

    # TODO: make it fail if writer_schema is provided?
    reader_schema_obj = _parse_fastavro_schema(reader_schema) if reader_schema else None
    basic_objs = fastavro.read.reader(stream, reader_schema=reader_schema_obj)
    return basic_objs


def _default_schema(py_type: Type) -> bytes:
    """Generate an Avro schema for a given Python type"""
    import py_avro_schema as pas

    # JSON as string matches default argument in to_basic_type function
    schema = pas.generate(py_type, options=pas.Option.LOGICAL_JSON_STRING)
    return schema


@functools.lru_cache(maxsize=100)
def _parse_fastavro_schema(json_data: bytes) -> fastavro.types.Schema:
    """Parse an Avro schema (JSON bytes) into a fastavro-internal representation"""
    return fastavro.parse_schema(orjson.loads(json_data))
