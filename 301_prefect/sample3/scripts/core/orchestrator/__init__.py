# scripts/core/orchestrator/__init__.py

"""
Orchestrator Module

This module is responsible for orchestrating the ETL pipeline execution.
It uses Prefect to define and run the sequence of tasks (steps)
that constitute an entire ETL flow.
"""

# Import the main pipeline running class to make it directly accessible.
from .flow_executor import FlowExecutor

# Import the utility function that wraps plugin execution logic into
# a Prefect task.
from .task_wrapper import wrap_as_task

# Define the public API for the 'orchestrator' package.
__all__ = [
    'FlowExecutor',
    'wrap_as_task',
]