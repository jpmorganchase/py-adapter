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
def serialize(obj: py_adapter.Basic, stream: BinaryIO) -> BinaryIO:
    """
    Serialize an object of basic Python types as JSON bytes

    :param obj:    Python object to serialize
    :param stream: File-like object to serialize data to
    """
    import orjson

    data = orjson.dumps(obj)
    stream.write(data)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def serialize_many(objs: Iterable[py_adapter.Basic], stream: BinaryIO) -> BinaryIO:
    """
    Serialize multiple Python objects of basic types as Newline Delimited JSON (NDJSON).

    :param objs:   Python objects to serialize
    :param stream: File-like object to serialize data to
    """
    import orjson

    data = b"\n".join(orjson.dumps(obj) for obj in objs)
    stream.write(data)
    stream.flush()
    return stream


@py_adapter.plugin.hook
def deserialize(stream: BinaryIO) -> py_adapter.Basic:
    """
    Deserialize JSON bytes as an object of basic Python types

    :param stream: File-like object to deserialize
    """
    import orjson

    return orjson.loads(stream.read())


@py_adapter.plugin.hook
def deserialize_many(stream: BinaryIO) -> Iterator[py_adapter.Basic]:
    """
    Deserialize Newline Delimited JSON (NDJSON) data as an iterator over objects of basic Python types

    :param stream: File-like object to deserialize
    """
    import orjson

    lines = stream.read().splitlines()
    return (orjson.loads(line) for line in lines)
