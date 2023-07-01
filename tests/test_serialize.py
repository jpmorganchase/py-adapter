import io

import pytest

import py_adapter


def tsst_invalid_format(ship_obj):
    with pytest.raises(..., match="'does not exist' serialization format not supported"):
        py_adapter.serialize(ship_obj, "does not exist")


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
    data = py_adapter.serialize(ship_obj, format="Avro")
    obj_out = py_adapter.deserialize(data, "Avro", ship_class)
    assert obj_out == ship_obj


def test_serialize_stream_json(ship_obj, ship_class):
    data = io.BytesIO()
    py_adapter.serialize_to_stream(ship_obj, data, format="JSON")
    data.seek(0)
    obj_out = py_adapter.deserialize_from_stream(data, ship_class, format="JSON")
    assert obj_out == ship_obj


def test_serialize_stream_avro(ship_obj, ship_class):
    data = io.BytesIO()
    py_adapter.serialize_to_stream(ship_obj, data, format="Avro")
    data.seek(0)
    obj_out = py_adapter.deserialize_from_stream(data, ship_class, format="Avro")
    assert obj_out == ship_obj


def test_serialize_many_json(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = py_adapter.serialize_many(ship_objs, format="JSON")
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="JSON"))
    assert objs_out == ship_objs


def test_serialize_many_avro(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = py_adapter.serialize_many(ship_objs, format="Avro")
    objs_out = list(py_adapter.deserialize_many(data, ship_class, format="Avro"))
    assert objs_out == ship_objs


def test_serialize_many_stream_json(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = io.BytesIO()
    py_adapter.serialize_many_to_stream(ship_objs, data, format="JSON")
    objs_out = list(py_adapter.deserialize_many_from_stream(data, ship_class, format="JSON"))
    assert objs_out == ship_objs


def test_serialize_many_stream_avro(ship_obj, ship_class):
    ship_objs = [ship_obj, ship_obj]
    data = io.BytesIO()
    py_adapter.serialize_many_to_stream(ship_objs, data, format="Avro")
    objs_out = list(py_adapter.deserialize_many_from_stream(data, ship_class, format="Avro"))
    assert objs_out == ship_objs
