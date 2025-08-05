# scripts/core/pipeline/pipeline_parser.py

import yaml
from typing import List, Dict, Any, Optional

class PipelineParser:
    """
    Parses a pipeline configuration YAML file.

    This class is responsible for loading a YAML file that defines the
    ETL pipeline, validating its basic structure, and providing easy
    access to its contents, such as the pipeline name and the list of steps.
    """

    def __init__(self, config_path: str):
        """
        Initializes the parser and loads the configuration from the given path.

        Args:
            config_path (str): The file path to the YAML configuration file.

        Raises:
            FileNotFoundError: If the configuration file cannot be found.
            yaml.YAMLError: If the file is not valid YAML.
            ValueError: If the configuration is missing essential keys.
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file {config_path}: {e}")

        if not isinstance(self.config, dict):
            raise ValueError("The root of the YAML configuration must be a dictionary.")

    def get_pipeline_name(self) -> Optional[str]:
        """
        Retrieves the name of the pipeline from the configuration.

        It looks for a top-level 'name' key in the configuration file.

        Returns:
            Optional[str]: The name of the pipeline, or None if not specified.
        """
        return self.config.get('name')

    def get_pipeline_steps(self) -> List[Dict[str, Any]]:
        """
        Retrieves the list of steps to be executed in the pipeline.

        It looks for a top-level 'pipeline' key, which should contain a list
        of step configurations.

        Returns:
            List[Dict[str, Any]]: A list where each item is a dictionary
                                  representing a single step's configuration.

        Raises:
            ValueError: If the 'pipeline' key is missing or is not a list.
        """
        steps = self.config.get('pipeline')
        if steps is None:
            raise ValueError("Configuration file must contain a 'pipeline' key.")
        
        if not isinstance(steps, list):
            raise ValueError("The 'pipeline' key must contain a list of steps.")
            
        return steps