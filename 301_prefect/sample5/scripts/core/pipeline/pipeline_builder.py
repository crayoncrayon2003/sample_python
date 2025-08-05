# scripts/core/pipeline/dag_pipeline_builder.py

from __future__ import annotations
from typing import Dict, Any, Optional
from prefect import task
from prefect.futures import PrefectFuture

from ..data_container.container import DataContainer
from .step_executor import StepExecutor

class DagPipelineBuilder:
    """
    Builds and orchestrates an ETL pipeline as a Directed Acyclic Graph (DAG) using Prefect.
    """

    def __init__(self, name: str):
        self.name = name
        self._run_step_task = task(StepExecutor().execute_step)
        self._nodes: Dict[str, PrefectFuture] = {}
        print(f"DagPipelineBuilder initialized for '{self.name}'.")

    def add_node(
        self,
        node_id: str,
        plugin: str,
        params: Dict[str, Any] | None = None,
        upstream_nodes: Dict[str, PrefectFuture] | None = None
    ) -> PrefectFuture:
        """
        Adds a new node (a Prefect task) to the DAG.
        """
        if node_id in self._nodes:
            raise ValueError(f"A node with id '{node_id}' already exists in this pipeline.")

        step_config = {"name": node_id, "plugin": plugin, "params": params or {}}

        # 1. First, apply options to the task to get a new, configured task object.
        configured_task = self._run_step_task.with_options(
            name=f"Step: {node_id}" # Use 'name' for the task run name in with_options
        )

        # 2. Then, call .submit() on the configured task object.
        future = configured_task.submit(
            step_config=step_config,
            upstream_results=upstream_nodes
        )

        self._nodes[node_id] = future

        print(f"  Node '{node_id}' (Plugin: {plugin}) submitted to Prefect.")

        return future

    def run(self):
        """
        (Placeholder) In this Prefect 2-based design, the DAG is executed
        as nodes are added within a @flow. This method is not required for execution.
        """
        print(f"DAG '{self.name}' has been defined. Prefect is managing the execution.")