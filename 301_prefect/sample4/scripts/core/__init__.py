# scripts/core/__init__.py

"""
ETL Framework Core Module

This module serves as the primary entry point for the core functionalities
of the ETL framework. It aggregates key components from its sub-packages,
providing a simplified and unified interface for developers building
ETL flows programmatically.
"""

# Expose key components from the data_container module
from .data_container.container import DataContainer
from .data_container.formats import SupportedFormats

# Expose key components from the config module
from .config.loader import ConfigLoader
from .config.validator import ConfigValidator

# Expose key components from the plugin_manager module
from .plugin_manager.interfaces import (
    PluginInterface,
    ExtractorInterface,
    CleanserInterface,
    TransformerInterface,
    ValidatorInterface,
    LoaderInterface
)
# Note: PluginManager and PluginRegistry are mostly used internally,
# so we don't need to expose them at the top level.

# Expose the main builder class from the pipeline module
from .pipeline.pipeline_builder import PipelineBuilder

# Expose key components from the orchestrator module
from .orchestrator.task_runner import run_pipeline_as_task


# Define the public API of the 'core' package using __all__
__all__ = [
    # data_container
    'DataContainer',
    'SupportedFormats',

    # config
    'ConfigLoader',
    'ConfigValidator',

    # plugin_manager (Interfaces are the most important public part)
    'PluginInterface',
    'ExtractorInterface',
    'CleanserInterface',
    'TransformerInterface',
    'ValidatorInterface',
    'LoaderInterface',

    # pipeline
    'PipelineBuilder',

    # orchestrator
    'run_pipeline_as_task',
]