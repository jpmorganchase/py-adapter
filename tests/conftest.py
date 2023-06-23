"""
Test data models
"""
import dataclasses
import datetime
import enum
import uuid
from typing import Any, Dict, List, Optional, Union

import pytest


class ShipType(enum.Enum):
    """A ship type"""

    SAILING_VESSEL = "SAILING_VESSEL"
    MOTOR_VESSEL = "MOTOR_VESSEL"


@dataclasses.dataclass
class Person:
    """A person on board the ship"""

    name: str = ""


@dataclasses.dataclass
class Goods:
    """Goods you can transport"""

    description: str = ""
    weight_kg: float = 0.0


@dataclasses.dataclass
class Engine:
    """A ship's engine"""

    power_kw: int = 0


@dataclasses.dataclass
class ElectricEngine(Engine):
    """Modern stuff"""

    voltage: int = 0


@dataclasses.dataclass
class DieselEngine(Engine):
    """Smelly stuff"""


class Tag(str):
    """Something that subclasses a primitive data type"""


@dataclasses.dataclass
class Ship:
    """A ship"""

    name: str  # Required field!
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    type: Optional[ShipType] = None
    crew: List[Person] = dataclasses.field(default_factory=list)
    cargo: Optional[Goods] = None
    departed_at: Optional[datetime.datetime] = None
    build_on: Optional[datetime.date] = None
    engine: Union[None, DieselEngine, ElectricEngine] = None
    sails: Optional[Dict[str, Any]] = dataclasses.field(
        default=None,
        metadata={
            "py_adapter": {"logical_type": "json"},
        },
    )
    tags: List[Tag] = dataclasses.field(default_factory=list)


class Port:
    """A port, not using dataclass"""

    def __init__(
        self, name: str, *, country: str = "NLD", latitude: float, longitude: float
    ):  # note the non-default kwargs near the end!
        self.name = name
        self.country = country.upper()
        self.latitude = latitude
        self.longitude = longitude


@pytest.fixture
def ship_obj():
    crew = [
        Person(name="Florenz"),
        Person(name="Cara"),
    ]
    cargo = Goods(description="Barrels of rum", weight_kg=5100.5)
    departed_at = datetime.datetime(2020, 10, 28, 13, 30, 0, tzinfo=datetime.timezone.utc)
    build_on = datetime.date(1970, 12, 31)
    sails = {
        "main": {"type": "dacron"},
        "jib": "white",
    }
    tags = [Tag("keelboat")]
    ship = Ship(
        id=uuid.UUID(int=1),
        name="Elvira",
        type=ShipType.SAILING_VESSEL,
        crew=crew,
        cargo=cargo,
        departed_at=departed_at,
        build_on=build_on,
        sails=sails,
        tags=tags,
    )
    return ship


@pytest.fixture
def ship_dict():
    """Do not compute this from ship_obj as we want to be able to assert against them both"""
    return {
        "cargo": {
            "description": "Barrels of rum",
            "weight_kg": 5100.5,
        },
        "crew": [
            {"name": "Florenz"},
            {"name": "Cara"},
        ],
        "departed_at": datetime.datetime(2020, 10, 28, 13, 30, tzinfo=datetime.timezone.utc),
        "build_on": datetime.date(1970, 12, 31),
        "engine": None,
        "id": "00000000-0000-0000-0000-000000000001",
        "name": "Elvira",
        "type": "SAILING_VESSEL",
        "sails": '{"main":{"type":"dacron"},"jib":"white"}',
        "tags": ["keelboat"],
    }


@pytest.fixture
def person_obj():
    return Person(name="Cara")
