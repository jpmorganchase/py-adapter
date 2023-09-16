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
Round-trip serialization/deserialization of any Python object to/from any serialization format including Avro and JSON.
"""

import abc
import copy
import dataclasses
import datetime
import enum
import importlib
import inspect
import logging
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

import avro.schema
import dateutil.parser
import memoization
import orjson
import py_avro_schema as pas

import py_adapter._schema
import py_adapter.plugin

try:
    from importlib import metadata
except ImportError:  # pragma: no cover
    # Python < 3.8
    import importlib_metadata as metadata  # type: ignore


#: Library version, e.g. 1.0.0, taken from Git tags
__version__ = metadata.version("py-adapter")


logger = logging.getLogger(__package__)

# Elementary serializable data types
Primitives = Union[None, bool, str, int, float]
Logicals = Union[datetime.datetime, datetime.date]
Record = Dict[str, "Basic"]
Array = List["Basic"]
Basic = Union[Primitives, Logicals, Array, Record]


# TODO: support datetime as nanosecond integer
def to_basic_type(obj: Any, *, datetime_type: Type = datetime.datetime, json_type: Type = str) -> Basic:
    """
    Convert an object into a data structure using "basic" types only as a pre-serialization step.

    :param obj:           The object to convert
    :param datetime_type: The type to convert datetime objects to. Supported types include :class:`int` (timestamp),
                          :class:`str` (ISO-format), and :class:`datetime.datetime` (no conversion).
    :param json_type:     The type to convert dataclass "JSON" dict fields to. Fields are identified by having custom
                          meta data ``{"py_adapter": {"logical_type": "json" }}``. If set to ``str`` (default), field
                          values are serialized as JSON strings. Set to ``dict`` for no conversion.
    """
    data_dict = _DictAdapter(
        datetime_type=datetime_type,
        json_type=json_type,
    ).adapt(obj)
    return data_dict


Obj = TypeVar("Obj")


def from_basic_type(basic_obj: Basic, py_type: Type[Obj]) -> Obj:
    """
    Convert a data structure with "basic" types only into a Python object of a given type

    :param basic_obj: Any valid data structure that can be used to create an instance of ``py_type``
    :param py_type:   The Python class to create an instance from
    """
    adapter = _ObjectAdapter.for_py_type(py_type)
    obj = adapter.adapt(basic_obj)
    return obj


def serialize(obj: Any, *, format: str, writer_schema: bytes = b"") -> bytes:
    """
    Serialize an object using a serialization format supported by **py-adapter**

    :param obj:           Python object to serialize
    :param format:        Serialization format as supported by a **py-adapter** plugin, e.g. ``JSON``.
    :param writer_schema: Data schema to serialize the data with, as JSON bytes.
    """
    serialize_fn = py_adapter.plugin.plugin_hook(format, "serialize")
    basic_obj = to_basic_type(obj)
    data = serialize_fn(obj=basic_obj, writer_schema=writer_schema)
    return data


def serialize_many(objs: Sequence[Any], *, format: str, writer_schema: bytes = b"") -> bytes:
    """
    Serialize multiple objects using a serialization format supported by **py-adapter**

    :param objs:          Python objects to serialize
    :param format:        Serialization format as supported by a **py-adapter** plugin, e.g. ``JSON``.
    :param writer_schema: Data schema to serialize the data with, as JSON bytes.
    """
    serialize_fn = py_adapter.plugin.plugin_hook(format, "serialize_many")
    basic_objs = [to_basic_type(obj) for obj in objs]
    data = serialize_fn(objs=basic_objs, writer_schema=writer_schema)
    return data


def deserialize(data: bytes, py_type: Type[Obj], *, format: str, writer_schema: bytes = b"") -> Obj:
    """
    Deserialize bytes as a Python object of a given type from a serialization format supported by **py-adapter**

    :param data:          Serialized data
    :param py_type:       The Python class to create an instance from
    :param format:        Serialization format as supported by a **py-adapter** plugin, e.g. ``JSON``.
    :param writer_schema: Data schema used to serialize the data with, as JSON bytes.
    """
    deserialize_fn = py_adapter.plugin.plugin_hook(format, "deserialize")
    basic_obj = deserialize_fn(data=data, writer_schema=writer_schema)
    obj = from_basic_type(basic_obj, py_type)
    return obj


class _Adapter(abc.ABC):
    """Interface for an adapter"""

    @abc.abstractmethod
    def adapt(self, data: Any) -> Any:
        """
        Adapt a data structure into something else

        :param data: A valid data structure
        """
        raise NotImplementedError()


class _DictAdapter(_Adapter):
    """An adapter to convert a Python object into a dict suitable for Avro serialization"""

    #: Whether to include private object fields in the dict
    incl_private_fields = False
    #: Whether to include keys in dicts starting with an underscore
    incl_private_keys = True

    def __init__(self, datetime_type: Type = datetime.datetime, json_type: Type = str):
        """
        :param datetime_type: The type to convert datetime objects to. Supported types include :class:`int` (timestamp),
                              :class:`str` (ISO-format), and :class:`datetime.datetime` (no conversion).
        :param json_type:     The type to convert dataclass "JSON" dict fields to. Fields are identified by having
                              custom meta data ``{"py_adapter": {"logical_type": "json" }}``. If set to ``str``
                              (default), field values are serialized as JSON strings. Set to ``dict`` for no conversion.
        """
        self.datetime_type = datetime_type
        self.json_type = json_type

    def adapt(self, data: Any) -> Basic:
        """
        Convert an object into a (nested) dictionary

        Logic adapted from :mod:`dataclasses` with additional logic added to handle :class:`enum.Enum` instances and
        JSON logical type fields inside data classes.

        :param data: The object to convert
        """
        if dataclasses.is_dataclass(data):
            return self._adapt_dataclass(data)
        elif isinstance(data, (list, tuple)):
            return list(self.adapt(v) for v in data)  # Additional logic: always use list
        elif isinstance(data, dict):
            # Modified: excluding private keys
            return type(data)(
                (self.adapt(k), self.adapt(v))
                for k, v in data.items()
                if not k.startswith("_") or self.incl_private_keys
            )
        elif isinstance(data, enum.Enum):  # Additional logic
            return data.value
        elif isinstance(data, datetime.datetime):
            return self._adapt_datetime(data)  # Additional logic
        elif isinstance(data, datetime.date):
            return self._adapt_date(data)  # Additional logic
        elif isinstance(data, str):
            return str(data)  # Additional logic, it might be a string subclass
        elif isinstance(data, uuid.UUID):  # Additional logic
            # TODO: introduce setting for UUID to str conversion, some serializer can work with UUID objects directly
            return str(data)
        else:
            try:
                # TODO: try dict(data) first
                return self.adapt(
                    {k: v for k, v in vars(data).items() if not k.startswith("_") or self.incl_private_fields}
                )  # Additional logic
            except TypeError:
                return copy.deepcopy(data)

    def _adapt_date(self, data: datetime.date) -> Union[Primitives, Logicals]:
        """Convert a date object"""
        if self.datetime_type == int:
            start_of_day = datetime.datetime.combine(data, datetime.time(), tzinfo=datetime.timezone.utc)
            return int(start_of_day.timestamp() * 1e3)  # Hardcode to timestamp in milliseconds for now
        elif self.datetime_type == str:
            return data.isoformat()
        else:
            return copy.deepcopy(data)

    def _adapt_datetime(self, data: datetime.datetime) -> Union[Primitives, Logicals]:
        """Convert a datetime object"""
        if self.datetime_type == int:
            return int(data.timestamp() * 1e3)  # Hardcode to timestamp in milliseconds for now
        elif self.datetime_type == str:
            return data.isoformat()
        else:
            return copy.deepcopy(data)

    def _adapt_dataclass(self, data: Any) -> Record:
        """Recursively convert a dataclass object with all fields"""
        result = []
        for f in dataclasses.fields(data):
            # Additional logic
            value: Basic
            field_meta = f.metadata.get(__package__)  # Retrieve dataclass field meta data relevant to current pkg
            if field_meta and field_meta.get("logical_type", "") == "json" and self.json_type == str:
                # If this is a Python dict field with logical type JSON, encode data as JSON
                value = orjson.dumps(getattr(data, f.name)).decode(encoding="utf-8")
            else:
                # Otherwise recursively adapt the field value as normal
                value = self.adapt(getattr(data, f.name))
            result.append((f.name, value))
        return dict(result)


class _ObjectAdapter(_Adapter):
    """An adapter to convert a dict into a Python object using an Avro schema"""

    #: Avro schema attribute to use for importing Python classes. Note that the ``namespace`` attribute is always used
    #: as a fallback and in Python-only environments it is recommended to set the Avro namespace as the Python package/
    #: module name.
    module_schema_attribute: str = "pyModule"
    #: Avro schema attribute for constructing Python objects (e.g. string subclasses) for Avro string primitive schemas.
    named_string_attribute: str = "namedString"

    def __init__(self, schema: avro.schema.Schema):
        """
        An adapter to convert a dict into a Python object using an Avro schema

        :param schema: The Avro schema to be used to adapt the data structure.
        """
        self.schema = schema

    @classmethod
    def for_py_type(cls, py_type: Type) -> "_ObjectAdapter":
        """
        An adapter to convert a dict into a Python object of a given class

        :param py_type: The Python class to return an object adapter for
        """
        try:
            # TODO: expose options as necessary
            schema = avro.schema.parse(
                pas.generate(py_type, options=pas.Option.LOGICAL_JSON_STRING | pas.Option.MILLISECONDS).decode("utf-8")
            )
        except pas.TypeNotSupportedError:
            raise TypeError(f"{py_type} not supported by py-adapter since it is not supported by py-avro-schema")
        return cls(schema)

    def adapt(self, data: Basic) -> Any:
        """
        Parse a data structure and return a Python object

        :param data: Any valid data structure that can be parsed using :attr:`schema`.
        """
        return self._parse(data, self.schema)

    def _parse(self, data: Basic, schema: avro.schema.Schema) -> Any:
        """Main parser method, called recursively"""
        # TODO: improve type hints, second callable argument must be a schema object
        parsers_by_schema: Dict[Type[avro.schema.Schema], Callable[[Any, Any], Any]] = {
            avro.schema.ArraySchema: self._parse_array,
            avro.schema.EnumSchema: self._parse_enum,
            avro.schema.UnionSchema: self._parse_union,
            avro.schema.RecordSchema: self._parse_record,
            avro.schema.TimestampMillisSchema: self._parse_timestamp_millis,
            avro.schema.DateSchema: self._parse_date,
            avro.schema.PrimitiveSchema: self._parse_primitive,
            avro.schema.UUIDSchema: self._parse_uuid,
        }
        parser = parsers_by_schema.get(type(schema))
        if parser:
            return parser(data, schema)
        else:
            return data

    def _parse_primitive(self, data: Primitives, schema: avro.schema.PrimitiveSchema) -> Any:
        """
        Parse primitive data types

        Primitives would typically map 1-to-1 to Python types and Avro logical types would be handled by the Avro
        serializer. However, we define here a custom string logical type "json" which maps to a Python dict type.
        """
        if schema.type == "string" and isinstance(data, str):
            if schema.get_prop("logicalType") == "json":
                try:
                    return orjson.loads(data.encode(encoding="utf-8"))
                except orjson.JSONDecodeError:  # Handling say empty string as input which is not valid JSON
                    # The Python object should infill the default value, e.g. dict or list.
                    # TODO: what happens if the Python object does not have a default?
                    return None
            elif schema.get_prop(self.named_string_attribute):
                # We want this to fail if named_string class is not importable
                dotted_name = cast(str, schema.get_prop(self.named_string_attribute))
                class_ = self._import_attribute(dotted_name)
                return class_(data)  # Instantiate class, which must be a subclass of str
        return data  # Avro serializer handles the rest

    def _parse_uuid(self, data: Union[str, uuid.UUID], schema: avro.schema.UUIDSchema) -> Union[None, uuid.UUID]:
        """
        Parse a UUID string as a Python UUID object
        """
        # TODO: introduce UUID to str conversion setting so we know whether the deserializer for a given format can
        # handle UUID objects itself.
        if isinstance(data, uuid.UUID):
            return data
        elif data:
            return uuid.UUID(data)
        else:
            # Accept an empty string and return None such that the Python class can initialize a value. Any
            # malformed strings would raise ValueErrors if they can't be cast as a UUID.
            return None

    def _parse_date(self, data: Union[datetime.date, int, str], schema: avro.schema.DateSchema) -> datetime.date:
        """
        Parse the int logical type "date".

        This is actually handled by the  Avro deserializer itself, but we also want to support raw integers that were
        not deserialized to a date, e.g. if we're deserializing from JSON. Handle that case directly here.
        """
        if isinstance(data, int):
            # Assume we have serialized as a timestamp in milliseconds. Now that is NOT how we would have serialized it
            # if it was Avro. So this is useful only in combination with serializing using ``datetime_type=int``.
            return datetime.date.fromtimestamp(data / 1e3)
        elif isinstance(data, str):
            return dateutil.parser.isoparse(data).date()
        else:
            # Deserialization to date object handled by Avro deserializer
            return data

    def _parse_timestamp_millis(
        self, data: Union[datetime.datetime, int, str], schema: avro.schema.TimestampMillisSchema
    ) -> datetime.datetime:
        """
        Parse the long logical type "millis".

        This is actually handled by the  Avro deserializer itself, but we also want to support raw integers that were
        not deserialized to a datetime, e.g. if we're deserializing from JSON. Handle that case directly here.
        """
        if isinstance(data, int):
            # Assume we have serialized as a timestamp in milliseconds. This is useful only in combination with
            # serializing using ``datetime_type=int``.
            return datetime.datetime.fromtimestamp(data / 1e3, tz=datetime.timezone.utc)
        elif isinstance(data, str):
            return dateutil.parser.isoparse(data)
        else:
            # Deserialization to datetime object handled by Avro deserializer
            return data

    def _parse_array(self, data: Array, schema: avro.schema.ArraySchema) -> List[Any]:
        """Parse an array/list schema"""
        data = [self._parse(elem, schema.items) for elem in data]
        return data

    def _parse_union(self, data: Basic, schema: avro.schema.UnionSchema) -> Any:
        """Parse a union schema trying the union branches one by one until the data fits"""

        # Rank the branch schemas by best matching then by position in the union
        scores_and_position = sorted(
            (-py_adapter._schema.match(s, data), position)  # Negative for best matched first
            for position, s in enumerate(schema.schemas)
        )
        # Remove branch schemas that do not match at all
        scores_and_position = [(s, p) for (s, p) in scores_and_position if s != 0]

        for _, position in scores_and_position:
            try:
                # Recursively parse data using best matched schema
                return self._parse(data, schema.schemas[position])
            except (TypeError, KeyError):
                # If record parsing fails, we try the next best schema
                continue
        else:
            # If none of the schemas matched the data at all
            raise DataTypeError(data, schema)

    def _parse_enum(self, data: str, schema: avro.schema.EnumSchema) -> Any:
        """Parse an enum schema"""
        enum_class = self._data_class(schema)
        if enum_class:
            return enum_class(data)
        else:  # TODO: remove this logic once we do proper writer vs reader schema resolution
            return None

    def _parse_record(self, data: Record, schema: avro.schema.RecordSchema) -> Any:
        """
        Parse a record/object schema

        This assumes objects satisfy the interface of :class:`dataclasses.dataclass`.
        """
        data_class = self._data_class(schema)
        if not data_class:  # TODO: remove this logic once we do proper writer vs reader schema resolution
            return None

        obj_kwargs = {
            field.name: self._parse(data[field.name], field.type) for field in schema.fields if field.name in data
        }
        factories = [
            self._obj_using_init,  # For proper dataclasses, this should work always
            self._obj_set_attrs,  # If a consumer has old dataclass code, there may be additional fields in data
            self._obj_init_first,  # Slow, use as last resort
        ]
        obj = error = None
        for factory in factories:
            try:
                obj = factory(data_class, obj_kwargs)
                break  # We're done, we've got an object!
            except (TypeError, KeyError) as e:  # Are there other errors we would need to catch?
                error = e  # Factory didn't work, move on to the next one
        if not obj and error:
            raise error from None  # None of the factories worked, raise the last error
        return obj

    @staticmethod
    def _obj_using_init(data_class: Type, kwargs: Dict[str, Basic]) -> Any:
        """Create an object by passing all fields into the constructor method"""
        obj = data_class(**kwargs)
        return obj

    def _obj_set_attrs(self, data_class: Type, kwargs: Dict[str, Basic]) -> Any:
        """Create an object by setting all fields directly after object instantiation"""
        obj = data_class()
        self._set_obj_fields(obj, kwargs)
        return obj

    def _obj_init_first(self, data_class: Type, kwargs: Dict[str, Basic]) -> Any:
        """
        Create an object by inspecting the constructor signature and passing all available parameters and setting other
        fields afterwards
        """
        # First inspect the data_class.__init__, find keyword only params vs the rest
        pos_args_names, kw_arg_names = _constructor_params(data_class)
        # Any other params that are in the payload but not in data_class.__init__
        remaining_arg_names = [name for name in kwargs if name not in pos_args_names and name not in kw_arg_names]

        # Take the corresponding parameter values from the payload
        pos_args = [kwargs[name] for name in pos_args_names if name in kwargs]
        kw_args = {name: kwargs[name] for name in kw_arg_names if name in kwargs}
        remaining_args = {name: kwargs[name] for name in remaining_arg_names}

        # Construct the object using data_class.__init__
        obj = data_class(*pos_args, **kw_args)
        # If possible set the remaining parameters directly as instance attributes
        self._set_obj_fields(obj, remaining_args)
        return obj

    @staticmethod
    def _set_obj_fields(obj, kwargs) -> None:
        """Set fields on an object if the object has the field"""
        for field, value in kwargs.items():
            if hasattr(obj, field):  # Set fields that exist only
                setattr(obj, field, value)

    def _data_class(self, schema: Union[avro.schema.RecordSchema, avro.schema.EnumSchema]) -> Optional[Type]:
        """
        Return the corresponding Python class for a schema

        This is relevant only for record and enum schemas. Python object is imported from a package taken from the
        schema's namespace or, optionally, from an schema attribute like ``pyModule``.
        """
        module_name = cast(str, schema.props.get(self.module_schema_attribute, schema.namespace))
        try:
            return getattr(importlib.import_module(module_name), schema.name)
        except AttributeError:  # TODO: remove this logic once we do proper writer vs reader schema resolution
            logger.warning("Failed to import class '%s.%s'", module_name, schema.name)
            return None

    @staticmethod
    def _import_attribute(dotted_name: str) -> Any:
        """Import and return attribute from a module, e.g. a class"""
        module_name, class_name = dotted_name.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)


@memoization.cached(max_size=100)
def _constructor_params(data_class):
    """Inspect __init__'s signature for a class and return (positional only params, other params)"""
    signature_params = inspect.signature(data_class.__init__).parameters
    pos_args_names = [
        name for name, param in signature_params.items() if param.kind == inspect.Parameter.POSITIONAL_ONLY
    ]
    kw_arg_names = [name for name, param in signature_params.items() if name not in pos_args_names]
    kw_arg_names.pop(0)  # Remove `self`, assuming self is not a positional only param!
    return pos_args_names, kw_arg_names


class DataTypeError(TypeError):
    """Data not compatible with the schema error"""

    def __init__(self, data: Any, schema: avro.schema.Schema) -> None:
        """Data not compatible with the schema error"""
        old_debug_validate = py_adapter._schema._DEBUG_VALIDATE
        py_adapter._schema._DEBUG_VALIDATE = True  # Patch avro package
        try:
            logger.error("Data '%s' is not compatible with schema %s. Validation output:", data, schema)
            py_adapter._schema.match(schema, data)  # This will print validation info to std out, not using logger
            super().__init__("Data not compatible with schema.")
        finally:
            py_adapter._schema._DEBUG_VALIDATE = old_debug_validate  # Unpatch
