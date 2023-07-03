"""

"""
import functools
import logging
import sys
from typing import TYPE_CHECKING

import pluggy

if TYPE_CHECKING:
    from pluggy._hooks import _HookCaller

    import py_adapter

logger = logging.getLogger(__package__)

#: Decorator for plugin hook functions
hook = pluggy.HookimplMarker(__package__)
#: Decorator for plugin hook function specifications/signatures
hookspec = pluggy.HookspecMarker(__package__)


@functools.lru_cache(maxsize=None)
def manager() -> pluggy.PluginManager:
    """
    Return a manager to discover and load plugins for providing hooks

    Plugins are automatically loaded through (setuptools) entrypoints, group ``inference_server``.
    """
    from py_adapter.plugin import avro, json

    logger.debug("Initializing plugin manager for '%s'", __package__)
    manager_ = pluggy.PluginManager(__package__)
    manager_.add_hookspecs(sys.modules[__name__])

    default_plugins = {
        "Avro": avro,
        "JSON": json,
    }
    for name, plugin in default_plugins.items():
        logger.debug("Loading default plugins '%s'", plugin)
        manager_.register(plugin, name=name)

    logger.debug("Discovering plugins using entrypoint group '%s'", __package__)
    manager_.load_setuptools_entrypoints(group=__package__)
    logger.debug("Loaded plugins: %s", manager_.get_plugins())
    return manager_


def plugin_hook(plugin_name: str, hook_name: str) -> "_HookCaller":
    """
    Return a hook (caller) for a single named plugin and hook name

    :param plugin_name: The name of the plugin to return the hook for
    :param hook_name:   The name of the hook function
    """
    pm = manager()
    all_plugins_except_this_one = (p for name, p in pm.list_name_plugin() if name != plugin_name)
    hook_caller = pm.subset_hook_caller(hook_name, remove_plugins=all_plugins_except_this_one)
    return hook_caller


@hookspec(firstresult=True)
def serialize(obj: "py_adapter.Basic") -> bytes:
    raise NotImplementedError()


@hookspec(firstresult=True)
def deserialize(data: bytes) -> "py_adapter.Basic":
    raise NotImplementedError()
