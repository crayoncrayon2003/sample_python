# scripts/core/pipeline/step_executor.py

from typing import Dict, Any, Optional

from ..plugin_manager.manager import PluginManager
from ..data_container.container import DataContainer
from ..plugin_manager.interfaces import PluginInterface, ExtractorInterface


class StepExecutor:
    """
    Executes a single step in the ETL pipeline.

    This class is responsible for taking a step's configuration,
    instantiating the appropriate plugin via the PluginManager, and then
    executing it with the provided data.
    """

    def __init__(self):
        """
        Initializes the StepExecutor.
        It creates an instance of the PluginManager to load plugins.
        """
        self.plugin_manager = PluginManager()

    def execute_step(
        self,
        step_config: Dict[str, Any],
        input_container: Optional[DataContainer]
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step based on its configuration.

        Args:
            step_config (Dict[str, Any]): A dictionary containing the
                configuration for the step (e.g., name, plugin, params).
            input_container (Optional[DataContainer]): The data container
                from the previous step. This is None for the first step.

        Returns:
            Optional[DataContainer]: The data container produced by the step's
                execution, which will be passed to the next step.
                Loaders might return None.
        
        Raises:
            ValueError: If the plugin name is not specified in the step config.
        """
        plugin_name = step_config.get('plugin')
        if not plugin_name:
            raise ValueError(f"'plugin' not specified in step: {step_config.get('name', 'Unnamed Step')}")

        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        print(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            # 1. Get the appropriate plugin instance from the manager
            plugin: PluginInterface = self.plugin_manager.get_plugin(
                name=plugin_name,
                params=params
            )

            # 2. Execute the plugin's main logic
            #    Extractor plugins are a special case as they create the first
            #    DataContainer and don't receive one.
            if isinstance(plugin, ExtractorInterface):
                output_container = plugin.execute()
            else:
                # For all other plugin types, pass the data from the previous step.
                output_container = plugin.execute(data=input_container)
            
            print(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            print(f"  ERROR during step '{step_name}': {e}")
            # Re-raise the exception to let Prefect handle the task failure
            raise