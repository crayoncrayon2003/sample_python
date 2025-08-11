# backend/plugins/transformers/with_jinja2.py

import json
from pathlib import Path
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class Jinja2Transformer:
    """
    (File-based) Transforms rows from a file into structured text using Jinja2.
    The output is a text file (e.g., JSON Lines).
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_jinja2"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input Parquet Path",
                    "description": "The Parquet file containing rows to be transformed."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Text File Path",
                    "description": "Path to save the rendered text output (e.g., a .jsonl file)."
                },
                "template_path": {
                    "type": "string",
                    "title": "Jinja2 Template Path",
                    "description": "The path to the .j2 template file."
                }
            },
            "required": ["input_path", "output_path", "template_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        template_path = Path(params.get("template_path"))

        if not all([input_path, output_path, template_path]):
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path', 'output_path', and 'template_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")
        if not template_path.exists():
            raise FileNotFoundError(f"Template file not found at: {template_path}")

        print(f"Reading file '{input_path}' to transform with template '{template_path.name}'...")
        df = pd.read_parquet(input_path)

        env = Environment(loader=FileSystemLoader(str(template_path.parent)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_path.name)

        print(f"Transforming {len(df)} rows and writing to '{output_path}'...")
        records = df.to_dict(orient='records')

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            for record in records:
                try:
                    rendered_string = template.render(record)
                    # To ensure valid JSON Lines, parse and dump each line
                    json_object = json.loads(rendered_string)
                    f.write(json.dumps(json_object) + '\n')
                except Exception as e:
                    print(f"ERROR rendering template for record: {record}. Error: {e}")
                    f.write(json.dumps({"error": str(e), "source_record": record}) + '\n')

        print("Transformation complete.")
        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container