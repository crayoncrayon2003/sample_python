# scripts/core/pipeline/step_executor.py

from typing import Dict, Any, Optional

from ..plugin_manager.manager import framework_manager
from ..data_container.container import DataContainer

class StepExecutor:
    """
    Executes a single step (node) in the ETL pipeline DAG by calling
    the appropriate plugin via the FrameworkManager.
    """

    # __init__でPluginManagerをインスタンス化する必要がなくなる
    # def __init__(self):
    #     self.plugin_manager = PluginManager()

    def execute_step(
        self,
        step_config: Dict[str, Any],
        inputs: Optional[Dict[str, DataContainer]] = None
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step by invoking the corresponding
        plugin hook via the FrameworkManager.

        Args:
            step_config (Dict[str, Any]): The configuration for the step.
            inputs (Optional[Dict[str, DataContainer]]): A dictionary of
                resolved DataContainer objects from upstream tasks.

        Returns:
            Optional[DataContainer]: The DataContainer produced by the step's execution.
        """
        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        if not plugin_name:
            raise ValueError(f"Step '{step_name}' is missing the 'plugin' key.")

        print(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            output_container = framework_manager.call_plugin_execute(
                plugin_name=plugin_name,
                params=params,
                inputs=inputs or {}
            )

            print(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            print(f"  ERROR during step '{step_name}': {e}")
            raise