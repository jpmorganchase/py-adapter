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
CSV serializer/deserializer **py-adapter** plugin
"""

import io
from typing import BinaryIO, Iterable, Iterator

import more_itertools

import py_adapter


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, stream: BinaryIO) -> BinaryIO:
    """
    Serialize an object of basic Python types as CSV data

    :param obj:    Python object to serialize
    :param stream: File-like object to serialize data to
    """
    import csv

    assert isinstance(obj, dict), "CSV serializer supports 'record' types only."
    text_stream = io.StringIO(newline="")  # csv modules writes as text
    csv_writer = csv.DictWriter(text_stream, fieldnames=obj.keys())
    csv_writer.writeheader()
    csv_writer.writerow(obj)
    text_stream.flush()
    text_stream.seek(0)

    stream.write(text_stream.read().encode("utf-8"))
    stream.flush()
    return stream


@py_adapter.plugin.hook
def serialize_many(objs: Iterable[py_adapter.Basic], stream: BinaryIO) -> BinaryIO:
    """
    Serialize multiple Python objects of basic types as CSV data

    :param objs:   Python objects to serialize
    :param stream: File-like object to serialize data to
    """
    import csv

    text_stream = io.StringIO(newline="")  # csv modules writes as text
    (first_obj,), objs = more_itertools.spy(objs)
    csv_writer = csv.DictWriter(text_stream, fieldnames=first_obj.keys())
    csv_writer.writeheader()
    csv_writer.writerows(objs)
    text_stream.flush()
    text_stream.seek(0)

    stream.write(text_stream.read().encode("utf-8"))
    stream.flush()
    return stream


@py_adapter.plugin.hook
def deserialize(stream: BinaryIO) -> py_adapter.Basic:
    """
    Deserialize CSV data as an object of basic Python types

    :param stream: File-like object to deserialize
    """
    import csv

    text_stream = io.StringIO(stream.read().decode("utf-8"))
    csv_reader = csv.DictReader(text_stream)
    obj = next(csv_reader)
    return obj


@py_adapter.plugin.hook
def deserialize_many(stream: BinaryIO) -> Iterator[py_adapter.Basic]:
    """
    Deserialize CSV data as an iterator over objects of basic Python types

    :param stream: File-like object to deserialize
    """
    import csv

    text_stream = io.StringIO(stream.read().decode("utf-8"))
    csv_reader = csv.DictReader(text_stream)
    return csv_reader
