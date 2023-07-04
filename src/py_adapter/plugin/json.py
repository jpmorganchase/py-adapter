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

import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic, writer_schema: bytes) -> bytes:
    """
    Serialize an object of basic Python types as JSON bytes

    :param obj:           Python object to serialize
    :param writer_schema: Schema to serialize the data with. Not used with JSON serialization.
    """
    import orjson

    return orjson.dumps(obj)


@py_adapter.plugin.hook
def deserialize(data: bytes) -> py_adapter.Basic:
    """
    Deserialize JSON bytes as an object of basic Python types

    :param data: JSON bytes to deserialize
    """
    import orjson

    return orjson.loads(data)
