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

import io
import re

import py_avro_schema as pas
import pytest

import py_adapter
import py_adapter.plugin


def test_invalid_format(ship_obj):
    with pytest.raises(
        py_adapter.plugin.InvalidFormat,
        match=re.escape(
            "A plugin for serialization format 'does not exist' is not available. Installed plugins/formats are: "
            "['Avro', 'JSON']."
        ),
    ):
        py_adapter.serialize(ship_obj, format="does not exist")


def test_plugin_without_hooks(ship_obj):
    class PluginNoHooks:
        """A plugin which does not implement required hooks"""

    pm = py_adapter.plugin.manager()
    pm.register(PluginNoHooks, "BrokenFormat")
    with pytest.raises(
        py_adapter.plugin.InvalidFormat,
        match=re.escape(
            "The plugin for serialization format 'BrokenFormat' does not implement the required hook 'serialize'."
        ),
    ):
        py_adapter.serialize(ship_obj, format="BrokenFormat")


def test_plugin_name_conflict(mocker):
    def patched_load_default_plugins(manager_):
        from py_adapter.plugin import _avro, _json

        manager_.register(_avro, name="format1")
        # Registering a plugin with conflicting name should fail
        manager_.register(_json, name="format1")

    mocker.patch("py_adapter.plugin._load_default_plugins", patched_load_default_plugins)
    py_adapter.plugin.manager.cache_clear()
    with pytest.raises(ValueError, match="Plugin name already registered: format1"):  # this is handled by Pluggy
        py_adapter.plugin.manager()


def test_serialize_json(ship_obj, ship_class):
    data = py_adapter.serialize(ship_obj, format="JSON")
    expected_serialization = (
        b'{"name":"Elvira","id":"00000000-0000-0000-0000-000000000001","type":"SAILING_VESSEL","crew":[{"name":'
        b'"Florenz"},{"name":"Cara"}],"cargo":{"description":"Barrels of rum","weight_kg":5100.5},"departed_at":'
        b'"2020-10-28T13:30:00+00:00","build_on":"1970-12-31","engine":null,"sails":'
        b'"{\\"main\\":{\\"type\\":\\"dacron\\"},\\"jib\\":\\"white\\"}","tags":["keelboat"]}'
    )
    assert data == expected_serialization
    obj_out = py_adapter.deserialize(data, ship_class, format="JSON")  # possibly auto-detect format
    assert obj_out == ship_obj


def test_serialize_avro(ship_obj, ship_class):
    writer_schema = pas.generate(ship_class, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS)
    data = py_adapter.serialize(ship_obj, format="Avro", writer_schema=writer_schema)
    obj_out = py_adapter.deserialize(data, ship_class, format="Avro", writer_schema=writer_schema)
    assert obj_out == ship_obj


def test_serialize_avro_automatic_writer_schema(ship_obj, ship_class):
    data = py_adapter.serialize(ship_obj, format="Avro")
    obj_out = py_adapter.deserialize(data, ship_class, format="Avro")
    assert obj_out == ship_obj


def test_serialize_stream_json(ship_obj, ship_class):
    data = io.BytesIO()
    py_adapter.serialize_to_stream(ship_obj, data, format="JSON")
    data.seek(0)
    obj_out = py_adapter.deserialize_from_stream(data, ship_class, format="JSON")
    assert obj_out == ship_obj


def test_serialize_stream_avro(ship_obj, ship_class):
    writer_schema = pas.generate(ship_class, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS)
    data = io.BytesIO()
    py_adapter.serialize_to_stream(ship_obj, data, format="Avro", writer_schema=writer_schema)
    data.seek(0)
    obj_out = py_adapter.deserialize_from_stream(data, ship_class, format="Avro", writer_schema=writer_schema)
    assert obj_out == ship_obj


def test_serialize_many_json(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = py_adapter.serialize_many(ship_objs, format="JSON")
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="JSON"))
    assert objs_out == ship_objs


def test_serialize_many_avro(ship_obj, ship_class):
    writer_schema = pas.generate(ship_class, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS)
    ship_objs = [ship_obj, ship_obj]
    data = py_adapter.serialize_many(ship_objs, format="Avro", writer_schema=writer_schema)
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="Avro"))
    assert objs_out == ship_objs


def test_serialize_many_avro_automatic_writer_schema(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = py_adapter.serialize_many(ship_objs, format="Avro")
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="Avro"))
    assert objs_out == ship_objs


def test_serialize_many_avro_automatic_writer_schema_generator(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]

    def ship_objs_generator():
        for ship in ship_objs:
            yield ship

    data = py_adapter.serialize_many(ship_objs_generator(), format="Avro")
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="Avro"))
    assert objs_out == ship_objs


def test_serialize_many_stream_json(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = io.BytesIO()
    py_adapter.serialize_many_to_stream(ship_objs, data, format="JSON")
    data.seek(0)
    objs_out = list(py_adapter.deserialize_many_from_stream(data, ship_class, format="JSON"))
    assert objs_out == ship_objs


def test_serialize_many_stream_avro(ship_obj, ship_class):
    writer_schema = pas.generate(ship_class, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS)
    ship_objs = [ship_obj, ship_obj]
    data = io.BytesIO()
    py_adapter.serialize_many_to_stream(ship_objs, data, format="Avro", writer_schema=writer_schema)
    data.seek(0)
    objs_out = list(
        py_adapter.deserialize_many_from_stream(data, ship_class, format="Avro", writer_schema=writer_schema)
    )
    assert objs_out == ship_objs
