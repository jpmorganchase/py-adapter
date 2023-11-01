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

from collections.abc import Iterable, Iterator
from typing import BinaryIO

import orjson

import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, stream: BinaryIO, writer_schema: bytes) -> BinaryIO:
    """
    Serialize an object of basic Python types as Avro bytes

    :param obj:           Python object to serialize
    :param stream:        File-like object to serialize data to
    :param writer_schema: Avro schema to serialize the data with, as JSON bytes.
    """
    import fastavro.write

    # TODO: generate schema if not provided
    schema_obj = fastavro.parse_schema(orjson.loads(writer_schema))
    # TODO: add support for writer which embeds the schema
    fastavro.write.schemaless_writer(stream, schema=schema_obj, record=obj)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def serialize_many(objs: Iterable[py_adapter.Basic], stream: BinaryIO, writer_schema: bytes) -> BinaryIO:
    """
    Serialize multiple Python objects of basic types as Avro container file format.

    :param objs:          Python objects to serialize
    :param stream:        File-like object to serialize data to
    :param writer_schema: Avro schema to serialize the data with, as JSON bytes.
    """
    import fastavro.write

    # TODO: generate schema if not provided
    schema_obj = fastavro.parse_schema(orjson.loads(writer_schema))
    fastavro.write.writer(stream, schema=schema_obj, records=objs)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def deserialize(stream: BinaryIO, writer_schema: bytes) -> py_adapter.Basic:
    """
    Deserialize Avro bytes as an object of basic Python types

    :param stream:        File-like object to deserialize
    :param writer_schema: Avro schema used to serialize the data with, as JSON bytes.
    """
    import fastavro.read

    # TODO: generate writer schema if not provided
    writer_schema_obj = fastavro.parse_schema(orjson.loads(writer_schema))
    # TODO: add support for reader schema, if provided
    # TODO: add support for reader of data with embedded (writer) schema
    basic_obj = fastavro.read.schemaless_reader(stream, writer_schema=writer_schema_obj, reader_schema=None)
    return basic_obj


@py_adapter.plugin.hook
def deserialize_many(stream: BinaryIO, writer_schema: bytes) -> Iterator[py_adapter.Basic]:
    """
    Deserialize Avro container file format data as an iterator over objects of basic Python types

    :param stream:        File-like object to deserialize
    :param writer_schema: Data schema used to serialize the data with, as JSON bytes.
    """
    import fastavro.read

    # TODO: make it fail if writer_schema is provided?
    # TODO: add support for reader schema, if provided
    basic_objs = fastavro.read.reader(stream, reader_schema=None)
    return basic_objs
