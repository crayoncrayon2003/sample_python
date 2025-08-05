# scripts/core/orchestrator/flow_executor.py

from typing import Optional

from ..pipeline.pipeline_parser import PipelineParser
from ..pipeline.step_executor import StepExecutor
from ..data_container.container import DataContainer

class FlowExecutor:
    """
    A simple, non-Prefect runner that executes a pipeline sequentially.
    Its job is to parse a config file and run the steps one by one.
    """

    def __init__(self, config_path: str):
        """
        Initializes the runner by parsing the configuration.
        """
        self.parser = PipelineParser(config_path)
        self.step_executor = StepExecutor()
        self.pipeline_name = self.parser.get_pipeline_name() or "Untitled ETL Pipeline"
        self.steps = self.parser.get_pipeline_steps()
        print(f"FlowExecutor initialized for '{self.pipeline_name}'.")

    def run(self):
        """
        Executes the entire pipeline from start to finish.
        """
        print(f"🚀 Starting pipeline run: '{self.pipeline_name}'")
        data_container: Optional[DataContainer] = None

        for i, step_config in enumerate(self.steps):
            step_name = step_config.get('name', f'step_{i+1}')
            
            try:
                data_container = self.step_executor.execute_step(
                    step_config=step_config,
                    input_container=data_container
                )
            except Exception as e:
                print(f"💥 Step '{step_name}' failed!")
                raise e # Re-raise the exception to be caught by the Prefect task

        print(f"✅ Pipeline '{self.pipeline_name}' finished successfully.")