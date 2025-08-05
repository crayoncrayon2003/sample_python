# scripts/core/pipeline/pipeline_builder.py

from __future__ import annotations # for returning self type hint in Python < 3.11

from typing import List, Dict, Any, Optional

from ..data_container.container import DataContainer
from ..pipeline.step_executor import StepExecutor

class PipelineBuilder:
    """
    Builds and executes an ETL pipeline programmatically using a fluent interface.

    This class allows developers to define a sequence of ETL steps in pure
    Python code, providing greater flexibility and better integration with IDEs
    compared to a YAML-based approach.
    """

    def __init__(self, name: str):
        """
        Initializes a new, empty pipeline.

        Args:
            name (str): The name of the pipeline, used for logging purposes.
        """
        self.name = name
        self._steps: List[Dict[str, Any]] = []
        self._step_executor = StepExecutor()
        print(f"PipelineBuilder initialized for '{self.name}'.")

    def add_step(
        self,
        plugin: str,
        name: str | None = None,
        params: Dict[str, Any] | None = None
    ) -> PipelineBuilder:
        """
        Adds a new step to the end of the pipeline.

        Args:
            plugin (str): The name of the plugin to use (must be in the registry).
            name (str | None, optional): A descriptive name for the step.
                If None, the plugin name is used. Defaults to None.
            params (Dict[str, Any] | None, optional): Parameters for the plugin.
                Defaults to None.

        Returns:
            PipelineBuilder: The instance of the builder itself, to allow for method chaining.
        """
        step_name = name or plugin
        step_config = {
            "name": step_name,
            "plugin": plugin,
            "params": params or {}
        }
        self._steps.append(step_config)
        print(f"  Added step: '{step_name}' (Plugin: {plugin})")
        return self

    def run(self) -> Optional[DataContainer]:
        """
        Executes the configured pipeline from start to finish.

        This method iterates through the added steps and executes them sequentially,
        passing the DataContainer from one step to the next.

        Returns:
            Optional[DataContainer]: The DataContainer from the final step of the pipeline,
                                     or None if the pipeline was empty or the last step
                                     was a loader.
        """
        print(f"🚀 Starting pipeline run: '{self.name}'")
        data_container: Optional[DataContainer] = None

        for step_config in self._steps:
            step_name = step_config.get('name')
            try:
                data_container = self._step_executor.execute_step(
                    step_config=step_config,
                    input_container=data_container
                )
            except Exception as e:
                print(f"💥 Step '{step_name}' failed!")
                raise e

        print(f"✅ Pipeline '{self.name}' finished successfully.")
        return data_container