# scripts/plugins/transformers/with_jinja2.py

import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from jinja2 import Environment, FileSystemLoader, Template
import pandas as pd
from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class Jinja2Transformer(BaseTransformer):
    """
    Transforms DataFrame rows into structured text (typically JSON) using Jinja2 templates.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.template_path = Path(self.params.get("template_path"))
        self.output_column_name = self.params.get("output_column_name", "rendered_json")

        if not self.template_path:
            raise ValueError("Jinja2Transformer requires a 'template_path' parameter.")
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found at: {self.template_path}")

        self.env = Environment(
            loader=FileSystemLoader(str(self.template_path.parent)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.template: Template = self.env.get_template(self.template_path.name)

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("Jinja2Transformer requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: Jinja2Transformer received a DataContainer with no DataFrame. Skipping.")
            return data

        source_df = data.data
        print(f"Transforming {len(source_df)} rows using Jinja2 template: {self.template_path.name}")

        records = source_df.to_dict(orient='records')
        rendered_results: List[str] = []
        for record in records:
            try:
                rendered_string = self.template.render(record)
                rendered_results.append(rendered_string)
            except Exception as e:
                print(f"ERROR rendering template for record: {record}")
                print(f"Error was: {e}")
                rendered_results.append(json.dumps({"error": str(e)}))

        result_df = pd.DataFrame(rendered_results, columns=[self.output_column_name])
        print(f"Transformation complete. Created a new DataFrame with {len(result_df)} rendered results.")

        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['jinja2_transformed'] = {
            'template': str(self.template_path)
        }

        return output_container