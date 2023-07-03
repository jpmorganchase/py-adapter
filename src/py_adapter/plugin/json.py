"""

"""
import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic) -> bytes:
    """
    Serialize an object of basic Python types as JSON bytes

    :param obj: Python object to serialize
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
