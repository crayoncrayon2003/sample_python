# scripts/core/pipeline/__init__.py

"""
Pipeline Module

This module is responsible for parsing the pipeline configuration,
managing the execution of individual steps, and resolving dependencies
between them. It forms the engine that drives the ETL process from
one step to the next.
"""

# Import the main class for parsing pipeline YAML files.
from .pipeline_parser import PipelineParser

# Import the main class responsible for executing a single pipeline step.
from .step_executor import StepExecutor

# Import the class for resolving dependencies (if any) between steps.
# This might be used for more complex, non-linear pipelines in the future.
from .dependency_resolver import DependencyResolver

# Define the public API for the 'pipeline' package.
__all__ = [
    'PipelineParser',
    'StepExecutor',
    'DependencyResolver',
]