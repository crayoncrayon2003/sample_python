# scripts/core/plugin_manager/manager.py

import importlib
from typing import Dict, Any

from .interfaces import PluginInterface
from .registry import plugin_registry  # The central registry of all plugins


class PluginManager:
    """
    Manages the lifecycle of plugins.

    This class acts as a factory for creating plugin instances. It uses the
    plugin registry to find the appropriate plugin class based on a name
    and then dynamically loads and instantiates it.
    """

    def get_plugin(self, name: str, params: Dict[str, Any]) -> PluginInterface:
        """
        Dynamically loads and instantiates a plugin by its registered name.

        This method performs the following steps:
        1. Looks up the plugin's full class path in the central plugin registry.
        2. Dynamically imports the module containing the plugin class.
        3. Retrieves the class object from the module.
        4. Instantiates the class with the provided parameters.

        Args:
            name (str): The registered name of the plugin (e.g., 'from_http').
            params (Dict[str, Any]): A dictionary of parameters to be passed
                                     to the plugin's constructor.

        Returns:
            PluginInterface: An instance of the requested plugin.

        Raises:
            ImportError: If the plugin is not registered, or if the class
                         or module cannot be found and imported.
        """
        # 1. Look up the full class path from the registry
        plugin_class_path = plugin_registry.get_plugin_path(name)
        if not plugin_class_path:
            raise ImportError(
                f"Plugin '{name}' is not registered. "
                f"Ensure it is added to the plugin_registry in 'scripts/core/plugin_manager/registry.py'."
            )

        try:
            # 2. Split the path into module path and class name
            module_path, class_name = plugin_class_path.rsplit('.', 1)
        except ValueError:
            raise ImportError(f"Invalid plugin path format for '{name}': '{plugin_class_path}'")

        try:
            # 3. Dynamically import the module
            module = importlib.import_module(module_path)
            
            # 4. Get the class from the loaded module
            plugin_class = getattr(module, class_name)

        except ImportError as e:
            raise ImportError(f"Could not import module '{module_path}' for plugin '{name}'. Original error: {e}")
        except AttributeError:
            raise ImportError(f"Class '{class_name}' not found in module '{module_path}' for plugin '{name}'.")

        # 5. Instantiate the plugin class with its parameters and return the instance
        # It's expected that all plugin constructors accept a 'params' dictionary.
        return plugin_class(params=params)