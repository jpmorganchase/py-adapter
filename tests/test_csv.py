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
Test script for CSV serialization/deserialization
"""

import dataclasses
import datetime
from typing import Optional

import pytest

import py_adapter


@dataclasses.dataclass
class SimpleShip:
    name: str
    build_on: Optional[datetime.date] = None


@pytest.fixture
def simple_ship():
    return SimpleShip(
        name="Elvira",
        build_on=datetime.date(1970, 12, 31),
    )


def test_serialize_1_record(simple_ship):
    data = py_adapter.serialize(simple_ship, format="CSV")
    expected_lines = [b"name,build_on", b"Elvira,1970-12-31"]
    assert data.splitlines() == expected_lines
    obj_out = py_adapter.deserialize(data, SimpleShip, format="CSV")
    assert obj_out == simple_ship


def test_serialize_many_records(simple_ship):
    objs_in = [simple_ship, simple_ship]
    data = py_adapter.serialize_many(objs_in, format="CSV")
    expected_lines = [b"name,build_on", b"Elvira,1970-12-31", b"Elvira,1970-12-31"]
    assert data.splitlines() == expected_lines
    objs_out = list(py_adapter.deserialize_many(data, SimpleShip, format="CSV"))
    assert objs_out == objs_in
