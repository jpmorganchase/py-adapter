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

import io

import orjson

import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, writer_schema: bytes) -> bytes:
    """
    Serialize an object of basic Python types as Avro bytes

    :param obj:           Python object to serialize
    :param writer_schema: Avro schema to serialize the data with, as JSON bytes.
    """
    import fastavro.write

    data = io.BytesIO()
    # TODO: generate schema if not provided
    schema_obj = fastavro.parse_schema(orjson.loads(writer_schema))
    fastavro.write.schemaless_writer(data, schema=schema_obj, record=obj)
    data.flush()
    data.seek(0)
    d = data.read()
    return d
