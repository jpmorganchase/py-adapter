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
JSON serializer/deserializer **py-adapter** plugin
"""
from collections.abc import Iterable, Iterator
from typing import BinaryIO

import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, stream: BinaryIO, writer_schema: bytes) -> BinaryIO:
    """
    Serialize an object of basic Python types as JSON bytes

    :param obj:           Python object to serialize
    :param writer_schema: Schema to serialize the data with. Not used with JSON serialization.
    """
    import orjson

    data = orjson.dumps(obj)
    stream.write(data)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def serialize_many(objs: Iterable[py_adapter.Basic], writer_schema: bytes) -> bytes:
    """
    Serialize multiple Python objects of basic types as Newline Delimited JSON (NDJSON).

    :param objs:          Python objects to serialize
    :param writer_schema: Schema to serialize the data with. Not used with JSON serialization.
    """
    import orjson

    return b"\n".join(orjson.dumps(obj) for obj in objs)


@py_adapter.plugin.hook
def deserialize(stream: BinaryIO, writer_schema: bytes) -> py_adapter.Basic:
    """
    Deserialize JSON bytes as an object of basic Python types

    :param data:          JSON bytes to deserialize
    :param writer_schema: Schema used to serialize the data with. Not used with JSON serialization.
    """
    import orjson

    return orjson.loads(stream.read())


@py_adapter.plugin.hook
def deserialize_many(data: bytes, writer_schema: bytes) -> Iterator[py_adapter.Basic]:
    """
    Deserialize Newline Delimited JSON (NDJSON) data as an iterator over objects of basic Python types

    :param data:          Bytes to deserialize
    :param writer_schema: Schema used to serialize the data with. Not used with JSON serialization.
    """
    import orjson

    return (orjson.loads(line) for line in data.splitlines())
