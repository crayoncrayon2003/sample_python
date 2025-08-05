# scripts/core/orchestrator/__init__.py

"""
Orchestrator Module

This module provides helper utilities to integrate the pipeline execution
with an orchestration engine like Prefect. It contains wrappers to
run a configured PipelineBuilder instance as a Prefect task.
"""

# Import the main wrapper function that runs a PipelineBuilder instance
# as a Prefect task.
from .task_runner import run_pipeline_as_task

# Define the public API for the 'orchestrator' package.
__all__ = [
    'run_pipeline_as_task',
]