"""

"""
import py_adapter
import py_adapter.plugin


@py_adapter.plugin.hook
def serialize(obj: py_adapter.Basic) -> bytes:
    """

    :param obj:
    """
    import orjson

    return orjson.dumps(obj)
