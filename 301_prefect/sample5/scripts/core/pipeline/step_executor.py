# scripts/core/pipeline/step_executor.py

from typing import Dict, Any, Optional

from ..plugin_manager.manager import PluginManager
from ..data_container.container import DataContainer
from ..plugin_manager.interfaces import PluginInterface, ExtractorInterface

class StepExecutor:
    """
    Executes a single step (node) in the ETL pipeline.
    """

    def __init__(self):
        self.plugin_manager = PluginManager()

    def execute_step(
        self,
        step_config: Dict[str, Any],
        inputs: Optional[Dict[str, DataContainer]] = None
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step with its resolved inputs.

        Args:
            step_config (Dict[str, Any]): The configuration for the step.
            inputs (Optional[Dict[str, DataContainer]]): A dictionary
                mapping input names to the resolved DataContainer objects
                from upstream tasks.

        Returns:
            Optional[DataContainer]: The DataContainer produced by the step's execution.
        """
        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        print(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            plugin: PluginInterface = self.plugin_manager.get_plugin(name=plugin_name, params=params)

            resolved_inputs = inputs or {}

            if isinstance(plugin, ExtractorInterface):
                output_container = plugin.execute(inputs={})
            else:
                output_container = plugin.execute(inputs=resolved_inputs)

            print(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            print(f"  ERROR during step '{step_name}': {e}")
            raise
