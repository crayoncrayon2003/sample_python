# scripts/core/__init__.py

"""
ETL Framework Core Module

This module serves as the primary entry point for the core functionalities
of the ETL framework. It aggregates key components from its sub-packages,
providing a simplified and unified interface for developers building
ETL flows.
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
from .plugin_manager.manager import PluginManager
from .plugin_manager.registry import PluginRegistry

# Expose key components from the pipeline module
from .pipeline.pipeline_parser import PipelineParser
from .pipeline.step_executor import StepExecutor
from .pipeline.dependency_resolver import DependencyResolver

# Expose key components from the orchestrator module
from .orchestrator.flow_executor import FlowExecutor
from .orchestrator.task_wrapper import wrap_as_task

# Define the public API of the 'core' package using __all__
# This controls what 'from scripts.core import *' imports.
__all__ = [
    # data_container
    'DataContainer',
    'SupportedFormats',

    # config
    'ConfigLoader',
    'ConfigValidator',

    # plugin_manager
    'PluginInterface',
    'ExtractorInterface',
    'CleanserInterface',
    'TransformerInterface',
    'ValidatorInterface',
    'LoaderInterface',
    'PluginManager',
    'PluginRegistry',

    # pipeline
    'PipelineParser',
    'StepExecutor',
    'DependencyResolver',

    # orchestrator
    'FlowExecutor',
    'wrap_as_task',
]