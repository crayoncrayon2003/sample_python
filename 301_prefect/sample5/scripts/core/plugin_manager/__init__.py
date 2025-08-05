# scripts/core/plugin_manager/__init__.py

"""
Plugin Manager Module

This module handles the discovery, registration, and management of
all plugins within the ETL framework. It provides the interfaces
(abstract base classes) that all plugins must adhere to and a manager
class to dynamically load them based on the pipeline configuration.
"""

# Import the abstract base classes that define the "contract" for each
# type of plugin. All plugins must inherit from one of these.
from .interfaces import (
    PluginInterface,
    ExtractorInterface,
    CleanserInterface,
    TransformerInterface,
    ValidatorInterface,
    LoaderInterface
)

# Import the main manager class that finds and instantiates plugins.
from .manager import PluginManager

# Import the registry class that holds the mapping from plugin names
# to their actual implementation classes.
from .registry import PluginRegistry

# Define the public API for the 'plugin_manager' package.
__all__ = [
    # Interfaces (the "rules" for plugins)
    'PluginInterface',
    'ExtractorInterface',
    'CleanserInterface',
    'TransformerInterface',
    'ValidatorInterface',
    'LoaderInterface',

    # Management classes
    'PluginManager',
    'PluginRegistry',
]