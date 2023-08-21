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
Plugin logic including plugin manager and hook specifications
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
_hookspec = pluggy.HookspecMarker(__package__)


@functools.lru_cache(maxsize=None)
def manager() -> pluggy.PluginManager:
    """
    Return a manager to discover and load plugins for providing hooks

    Plugins are automatically loaded through (setuptools) entrypoints, group ``inference_server``.
    """
    logger.debug("Initializing plugin manager for '%s'", __package__)
    manager_ = pluggy.PluginManager(__package__)
    manager_.add_hookspecs(sys.modules[__name__])

    _load_default_plugins(manager_)

    logger.debug("Discovering plugins using entrypoint group '%s'", __package__)
    manager_.load_setuptools_entrypoints(group=__package__)
    logger.debug("Loaded plugins: %s", manager_.get_plugins())
    return manager_


def _load_default_plugins(manager_: pluggy.PluginManager) -> None:
    """Load plugins that are packaged with py-adapter"""
    from py_adapter.plugin import _avro, _json

    default_plugins = {
        "Avro": _avro,
        "JSON": _json,
    }
    for name, plugin in default_plugins.items():
        logger.debug("Loading default plugin '%s'", plugin)
        manager_.register(plugin, name=name)


def plugin_hook(plugin_name: str, hook_name: str) -> "_HookCaller":
    """
    Return a hook (caller) for a single named plugin and hook name

    :param plugin_name: The name of the plugin to return the hook for
    :param hook_name:   The name of the hook function
    """
    pm = manager()
    all_plugins_except_this_one = (p for name, p in pm.list_name_plugin() if name != plugin_name)
    hook_caller = pm.subset_hook_caller(hook_name, remove_plugins=all_plugins_except_this_one)
    if not hook_caller.get_hookimpls():
        raise InvalidFormat(plugin_name=plugin_name, hook_name=hook_name)
    return hook_caller


class InvalidFormat(ValueError):
    """There is no plugin supporting the given format name"""

    def __init__(self, plugin_name: str, hook_name: str):
        """Initialize error with custom message"""
        pm = manager()
        if not pm.get_plugin(plugin_name):
            plugins_for_hook = sorted(impl.plugin_name for impl in getattr(pm.hook, hook_name).get_hookimpls())
            msg = (
                f"A plugin for serialization format '{plugin_name}' is not available. Installed plugins/formats are: "
                f"{plugins_for_hook}."
            )
        else:
            msg = (
                f"The plugin for serialization format '{plugin_name}' does not implement the required hook "
                f"'{hook_name}'."
            )
        super().__init__(msg)


@_hookspec(firstresult=True)
def serialize(obj: "py_adapter.Basic", writer_schema: bytes) -> bytes:
    """
    Hook specification. Serialize a Python object of basic types to the format supported by the implementing plugin.

    :param obj:           Python object to serialize
    :param writer_schema: Data schema to serialize the data with, as JSON bytes.
    """
    raise NotImplementedError()


@_hookspec(firstresult=True)
def deserialize(data: bytes, writer_schema: bytes) -> "py_adapter.Basic":
    """
    Hook specification. Deserialize data as an object of basic Python types

    :param data:          Bytes to deserialize
    :param writer_schema: Data schema used to serialize the data with, as JSON bytes.
    """
    raise NotImplementedError()
