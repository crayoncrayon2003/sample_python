# scripts/plugins/transformers/with_jinja2.py

import json
from pathlib import Path
from typing import Dict, Any, List

from jinja2 import Environment, FileSystemLoader, Template
from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class Jinja2Transformer(BaseTransformer):
    """
    Transforms DataFrame rows into structured text (typically JSON) using Jinja2 templates.

    This transformer iterates through each row of the input DataFrame, treating
    each row as a context dictionary to render a Jinja2 template. The output
    is a list of rendered strings, which are then stored as a single-column
    DataFrame in the output DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the transformer with a Jinja2 template.

        Expected params:
            - template_path (str): The path to the Jinja2 template file.
            - output_column_name (str, optional): The name of the column in the
              output DataFrame that will hold the rendered results.
              Defaults to 'rendered_json'.
        """
        super().__init__(params)
        self.template_path = Path(self.params.get("template_path"))
        self.output_column_name = self.params.get("output_column_name", "rendered_json")

        if not self.template_path:
            raise ValueError("Jinja2Transformer requires a 'template_path' parameter.")
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found at: {self.template_path}")

        # Set up the Jinja2 environment to load templates from the file's directory
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_path.parent)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.template: Template = self.env.get_template(self.template_path.name)

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Renders the Jinja2 template for each row in the input DataFrame.

        Args:
            data (DataContainer): The input container holding the DataFrame.

        Returns:
            DataContainer: A new container with a single-column DataFrame
                           containing the rendered text for each row.
        """
        if data.data is None:
            print("Warning: Jinja2Transformer received a DataContainer with no DataFrame. Skipping.")
            return data

        source_df = data.data
        print(f"Transforming {len(source_df)} rows using Jinja2 template: {self.template_path.name}")

        # Convert DataFrame to a list of dictionaries, where each dict is a row.
        # This format is easy to use as context in Jinja2.
        records = source_df.to_dict(orient='records')

        rendered_results: List[str] = []
        for record in records:
            try:
                # Render the template with the row's data as context
                rendered_string = self.template.render(record)
                rendered_results.append(rendered_string)
            except Exception as e:
                print(f"ERROR rendering template for record: {record}")
                print(f"Error was: {e}")
                # Optionally, add a placeholder for failed records
                rendered_results.append(json.dumps({"error": str(e)}))

        # Create a new DataFrame with a single column holding the results
        import pandas as pd
        result_df = pd.DataFrame(
            rendered_results,
            columns=[self.output_column_name]
        )

        print(f"Transformation complete. Created a new DataFrame with {len(result_df)} rendered results.")

        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['jinja2_transformed'] = {
            'template': str(self.template_path)
        }
        
        return output_container