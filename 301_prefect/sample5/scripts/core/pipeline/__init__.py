# scripts/core/pipeline/__init__.py

"""
Pipeline Module

This module provides the core components for building and executing
ETL pipelines. The central component is the `PipelineBuilder`, which
allows for the programmatic, code-based construction of a pipeline.
"""

# Import the main builder class for programmatic pipeline definition.
from .pipeline_builder import PipelineBuilder

# The StepExecutor is an internal component used by the PipelineBuilder,
# but it can be useful to expose it for advanced use cases or testing.
from .step_executor import StepExecutor

# DependencyResolver is kept for potential future use with non-linear pipelines.
from .dependency_resolver import DependencyResolver

# Define the public API for the 'pipeline' package.
__all__ = [
    'DagPipelineBuilder',
    'StepExecutor',
    'DependencyResolver',
]