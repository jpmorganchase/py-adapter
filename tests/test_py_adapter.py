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


import datetime
import io
import json
import pathlib
import re
import uuid

import avro.datafile
import avro.io
import avro.schema
import orjson
import py_avro_schema as pas
import pytest

import py_adapter
import py_adapter._schema


def test_package_has_version():
    assert py_adapter.__version__ is not None


@pytest.fixture(scope="session")
def ship_schema(ship_class):
    return avro.schema.parse(
        pas.generate(ship_class, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS).decode("utf-8")
    )


@pytest.fixture(scope="session")
def ship_adapter(ship_class):
    return py_adapter._ObjectAdapter.for_py_type(ship_class)


@pytest.fixture(scope="session")
def person_adapter(person_class):
    return py_adapter._ObjectAdapter.for_py_type(person_class)


@pytest.fixture(scope="session")
def port_adapter(port_class):
    return py_adapter._ObjectAdapter.for_py_type(port_class)


def test_serde_with_avro(ship_schema, ship_obj):
    """
    Round trip Avro serialization/deserialization test to demonstrate integration with Avro
    """
    # This is all that is required to convert a dataclass to a dict which we can the load into the datum writer
    ship_dict = py_adapter.to_basic_type(ship_obj)
    # Adding attributes that are not in the Avro schema will not work.
    # ship_dict["flag"] = "NLD"
    # ship_dict["crew"][0]["role"] = "Master"
    # ship_dict["crew"][1]["role"] = "Chief mate"

    binary_data = io.BytesIO()
    writer = avro.datafile.DataFileWriter(binary_data, avro.io.DatumWriter(writers_schema=ship_schema), ship_schema)
    writer.append(ship_dict)
    writer.flush()

    reader = avro.datafile.DataFileReader(binary_data, avro.io.DatumReader())
    read_dict = next(reader)

    # All that is required to parse the data again
    parser = py_adapter._ObjectAdapter(ship_schema)
    parsed_obj = parser.adapt(read_dict)

    assert parsed_obj == ship_obj


def test_serde_with_json(ship_schema, ship_obj):
    """
    Round trip JSON serialization/deserialization test to demonstrate integration with orjson
    """
    # This is all that is required to convert a dataclass to a dict which we can the load into the datum writer
    ship_dict = py_adapter.to_basic_type(ship_obj, datetime_type=int, json_type=dict)
    # These attributes are missing from the class object on purpose, for testing. Adding them here is fine since JSON
    # does not validate the payload against a schema. Upon deserialization though, this data will be dropped again.
    ship_dict["flag"] = "NLD"
    ship_dict["crew"][0]["role"] = "Master"
    ship_dict["crew"][1]["role"] = "Chief mate"

    json_data = orjson.dumps(ship_dict)
    read_dict = orjson.loads(json_data)
    assert isinstance(read_dict["departed_at"], int)
    assert isinstance(read_dict["sails"], dict)

    parser = py_adapter._ObjectAdapter(ship_schema)
    parsed_obj = parser.adapt(read_dict)

    assert parsed_obj == ship_obj
    assert not hasattr(parsed_obj, "flag")
    assert not hasattr(parsed_obj.crew[0], "role")
    assert not hasattr(parsed_obj.crew[1], "role")


def test_serde_dict_only(ship_class, ship_obj):
    ship_dict = py_adapter.to_basic_type(ship_obj)
    adapted_ship_obj = py_adapter.from_basic_type(ship_dict, ship_class)
    assert adapted_ship_obj == ship_obj


def test_to_basic_type(ship_obj, ship_dict):
    adapted_ship_dict = py_adapter.to_basic_type(ship_obj)
    assert adapted_ship_dict == ship_dict


def test_to_basic_type_json_with_underscore_fields(ship_obj):
    ship_obj.sails = {
        "_main": {"type": "dacron"},
    }
    adapted_ship_dict = py_adapter.to_basic_type(ship_obj, json_type=dict)
    assert adapted_ship_dict["sails"] == {
        "_main": {"type": "dacron"},
    }


def test_to_basic_type_json_excl_private_fields(ship_obj):
    ship_obj.sails = {
        "_main": {"type": "dacron"},
    }
    adapter = py_adapter._DictAdapter(json_type=dict)
    adapter.incl_private_keys = False
    adapted_ship_dict = adapter.adapt(ship_obj)
    assert adapted_ship_dict["sails"] == {}


def test_to_basic_type_obj_with_underscore_fields():
    class Ship:
        """Popo"""

    ship_obj = Ship()
    ship_obj._sails = 2
    adapter = py_adapter._DictAdapter(json_type=dict)
    adapter.incl_private_fields = True
    adapted_ship_dict = adapter.adapt(ship_obj)
    assert adapted_ship_dict["_sails"] == 2


def test_to_basic_type_obj_excl_private_fields():
    class Ship:
        """Popo"""

    ship_obj = Ship()
    ship_obj._sails = 2
    adapted_ship_dict = py_adapter.to_basic_type(ship_obj, json_type=dict)
    assert adapted_ship_dict.get("sails") is None


def test_from_basic_type(ship_obj, ship_dict, ship_class):
    adapted_ship_obj = py_adapter.from_basic_type(ship_dict, ship_class)
    assert adapted_ship_obj == ship_obj


def test_from_basic_type_bad_json_string(ship_obj, ship_dict, ship_class):
    ship_dict["sails"] = "{{not valid json}}"
    adapted_ship_obj = py_adapter.from_basic_type(ship_dict, ship_class)
    assert adapted_ship_obj.sails is None


def test_from_basic_type_empty_json_string(ship_obj, ship_dict, ship_class):
    ship_dict["sails"] = ""
    adapted_ship_obj = py_adapter.from_basic_type(ship_dict, ship_class)
    assert adapted_ship_obj.sails is None


def test_string_field(ship_adapter):
    data = {
        "name": "Elvira",
    }
    ship = ship_adapter.adapt(data)
    assert ship.name == "Elvira"


def test_enum_field(ship_adapter):
    data = {
        "name": "Elvira",
        "type": "SAILING_VESSEL",
    }
    ship = ship_adapter.adapt(data)
    assert ship.type.value == "SAILING_VESSEL"


def test_array_field(ship_adapter):
    data = {
        "name": "Elvira",
        "crew": [
            {"name": "Florenz"},
            {"name": "Cara"},
        ],
    }
    ship = ship_adapter.adapt(data)
    assert len(ship.crew) == 2
    assert ship.crew[0].name == "Florenz"
    assert ship.crew[1].name == "Cara"


def test_datetime_field(ship_adapter):
    departure_time = datetime.datetime.now(tz=datetime.timezone.utc)
    data = {
        "name": "Elvira",
        "departed_at": departure_time,
    }
    ship = ship_adapter.adapt(data)
    assert ship.departed_at == departure_time


def test_datetime_field_from_millis(ship_adapter):
    departure_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=123_000)  # Use milli precision
    data = {
        "name": "Elvira",
        "departed_at": int(departure_time.timestamp() * 1e3),  # Use millisecond precision
    }
    ship = ship_adapter.adapt(data)
    assert ship.departed_at == departure_time


def test_datetime_field_from_iso_string(ship_adapter):
    departure_time = datetime.datetime.now(tz=datetime.timezone.utc)
    data = {
        "name": "Elvira",
        "departed_at": departure_time.isoformat(),
    }
    ship = ship_adapter.adapt(data)
    assert ship.departed_at == departure_time


def test_union_with_null(ship_adapter):
    data = {
        "name": "Elvira",
        "cargo": {
            "description": "Barrels of rum",
            "weight_kg": 5100.50,
        },
    }
    ship = ship_adapter.adapt(data)
    assert ship.cargo.description == "Barrels of rum"
    assert pytest.approx(5100.5) == ship.cargo.weight_kg


def test_union_with_null_extra_field(ship_adapter):
    data = {
        "name": "Elvira",
        "cargo": {
            "description": "Barrels of rum",
            "weight_kg": 5100.50,
            "origin": "Aruba",
        },
    }
    ship = ship_adapter.adapt(data)
    assert ship.cargo.description == "Barrels of rum"
    assert pytest.approx(5100.5) == ship.cargo.weight_kg
    assert not hasattr(ship.cargo, "origin")


def test_union_with_null_missing_field(ship_adapter):
    data = {
        "name": "Elvira",
        "cargo": {
            "description": "Barrels of rum",  # weight_kg is missing now
        },
    }
    ship = ship_adapter.adapt(data)
    assert ship.cargo.description == "Barrels of rum"
    assert isinstance(ship.cargo.weight_kg, float)
    assert ship.cargo.weight_kg == 0.0  # should take default value from dataclass


def test_union_records(ship_adapter):
    data = {
        "name": "Elvira",
        "engine": {
            "power_kw": 20,
            "voltage": 48,  # Sounds like we're talking electric engine here!
        },
    }
    ship = ship_adapter.adapt(data)
    assert ship.engine.power_kw == 20
    assert ship.engine.voltage == 48


def test_union_records_ambiguous(ship_adapter):
    data = {
        "name": "Elvira",
        "engine": {
            "power_kw": 20,  # Could be electric or diesel!
        },
    }
    ship = ship_adapter.adapt(data)
    assert ship.engine.power_kw == 20
    assert not hasattr(ship.engine, "voltage")  # Adapter took first matching schema: DieselEngine!


def test_field_not_in_schema(ship_adapter):
    data = {
        "name": "Elvira",
        "home_port": "Rotterdam",
    }
    ship = ship_adapter.adapt(data)
    assert not hasattr(ship, "home_port")


def test_field_not_in_class(ship_adapter):
    data = {
        "name": "Elvira",
        "flag": "NLD",
    }
    ship = ship_adapter.adapt(data)
    assert not hasattr(ship, "flag")


def test_field_not_in_class_keep_optional_fields(ship_adapter):
    departure_time = datetime.datetime.now(tz=datetime.timezone.utc)
    data = {
        "name": "Elvira",
        "flag": "NLD",
        "departed_at": departure_time,
    }
    ship = ship_adapter.adapt(data)
    assert not hasattr(ship, "flag")
    assert ship.departed_at == departure_time


def test_field_not_in_class_optionals_only(person_adapter):
    data = {
        "name": "Cara",
        "role": "Chief mate",
    }
    person = person_adapter.adapt(data)
    assert not hasattr(person, "role")
    assert person.name == "Cara"


def test_non_dataclass(port_adapter):
    data = {
        "name": "Rotterdam",
        "country": "nld",
        "latitude": 51.981443,
        "longitude": -4.080739,
    }
    port = port_adapter.adapt(data)
    assert port.name == "Rotterdam"
    assert port.country == "NLD"
    assert pytest.approx(51.981443) == port.latitude
    assert pytest.approx(-4.080739) == port.longitude


def test_non_dataclass_extra_arg(port_adapter):
    data = {
        "name": "Rotterdam",
        "country": "nld",
        "latitude": 51.981443,
        "longitude": -4.080739,
        "is_tidal": True,
    }
    port = port_adapter.adapt(data)
    assert port.name == "Rotterdam"
    assert port.country == "NLD"
    assert pytest.approx(51.981443) == port.latitude
    assert pytest.approx(-4.080739) == port.longitude
    assert not hasattr(port, "is_tidal")


def test_data_type_error(ship_adapter):
    data = {
        "name": "Elvira",
        "type": "tug",
    }
    with pytest.raises(py_adapter.DataTypeError):
        ship_adapter.adapt(data)


def test_string_subclass(ship_adapter):
    data = {
        "name": "Elvira",
        "tags": ["longkeeler"],
    }
    ship = ship_adapter.adapt(data)
    import conftest

    assert isinstance(ship.tags[0], conftest.Tag)


def test_non_importable_data_type(caplog):
    file_names = [
        "country.avsc",  # There is no corresponding Python class for this
        "goods_with_country.avsc",
    ]
    json_datas = [json.loads((pathlib.Path(__file__).parent / file_name).read_text()) for file_name in file_names]
    names = avro.schema.Names()
    for json_data in json_datas:
        avro.schema.make_avsc_object(json_data, names=names)

    data = {
        "description": "Barrels of rum",
        "weight_kg": 5100.50,
        "country": {
            "code": "NLD",
        },
    }
    goods_adapter = py_adapter._ObjectAdapter(names.get_name("conftest.Goods", None))
    goods = goods_adapter.adapt(data)
    assert goods.description == "Barrels of rum"
    assert goods.weight_kg == pytest.approx(5100.5)
    assert not hasattr(goods, "country")
    assert "Failed to import class 'conftest.Country'" in caplog.text


def test_datetime_no_conversion():
    departure_time = datetime.datetime.now(tz=datetime.timezone.utc)
    assert py_adapter.to_basic_type(departure_time) == departure_time


def test_datetime_int():
    departure_time = datetime.datetime(1970, 1, 1, 0, 1, 0, tzinfo=datetime.timezone.utc)
    expected_serialization = 60 * 1_000  # Using milliseconds
    assert py_adapter.to_basic_type(departure_time, datetime_type=int) == expected_serialization
    assert py_adapter.from_basic_type(expected_serialization, datetime.datetime) == departure_time


def test_datetime_str():
    departure_time = datetime.datetime(1970, 1, 1, 0, 1, 0, tzinfo=datetime.timezone.utc)
    expected_serialization = "1970-01-01T00:01:00+00:00"
    assert py_adapter.to_basic_type(departure_time, datetime_type=str) == "1970-01-01T00:01:00+00:00"
    assert py_adapter.from_basic_type(expected_serialization, datetime.datetime) == departure_time


def test_date_no_conversion():
    departure_time = datetime.date.today()
    assert py_adapter.to_basic_type(departure_time) == departure_time


def test_date_int():
    departure_time = datetime.date(1970, 1, 2)
    expected_serialization = 24 * 60 * 60 * 1_000  # Using milliseconds
    assert py_adapter.to_basic_type(departure_time, datetime_type=int) == expected_serialization
    assert py_adapter.from_basic_type(expected_serialization, datetime.date) == departure_time


def test_date_str():
    departure_time = datetime.date(1970, 1, 2)
    expected_serialization = "1970-01-02"
    assert py_adapter.to_basic_type(departure_time, datetime_type=str) == expected_serialization
    assert py_adapter.from_basic_type(expected_serialization, datetime.date) == departure_time


def test_uuid():
    id = uuid.UUID(int=1)
    expected_serialization = "00000000-0000-0000-0000-000000000001"
    assert py_adapter.to_basic_type(id) == expected_serialization
    assert py_adapter.from_basic_type(expected_serialization, uuid.UUID) == id


def test_uuid_from_empty_string():
    assert py_adapter.from_basic_type("", uuid.UUID) is None


def test_unsupported_type():
    class Sail:
        def __init__(self, name):  # untyped __init__ arguments not supported
            self.name = name

    sail = Sail(name="spinnaker")
    expected_serialization = {"name": "spinnaker"}
    assert py_adapter.to_basic_type(sail) == expected_serialization  # This works
    with pytest.raises(
        TypeError,
        match=re.escape(
            "<class 'test_py_adapter.test_unsupported_type.<locals>.Sail'> not supported by py-adapter since it is not "
            "supported by py-avro-schema"
        ),
    ):
        py_adapter.from_basic_type(expected_serialization, Sail)  # This does not work
