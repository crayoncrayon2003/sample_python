# scripts/plugins/transformers/with_jinja2.py

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from jinja2 import Environment, FileSystemLoader, Template
import pandas as pd
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class Jinja2Transformer:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_jinja2"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        template_path = Path(params.get("template_path"))
        output_column_name = params.get("output_column_name", "rendered_json")
        if not template_path: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'template_path'.")
        if not template_path.exists(): raise FileNotFoundError(f"Template not found: {template_path}")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: Jinja2Transformer received no DataFrame.")
            return data

        env = Environment(loader=FileSystemLoader(str(template_path.parent)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_path.name)
        
        source_df = data.data
        print(f"Transforming {len(source_df)} rows using Jinja2 template: {template_path.name}")
        records = source_df.to_dict(orient='records')
        rendered_results: List[str] = []
        for record in records:
            try:
                rendered_results.append(template.render(record))
            except Exception as e:
                print(f"ERROR rendering template for record: {record}. Error: {e}")
                rendered_results.append(json.dumps({"error": str(e)}))

        result_df = pd.DataFrame(rendered_results, columns=[output_column_name])
        print(f"Transformation complete. Created new DataFrame with {len(result_df)} results.")

        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['jinja2_transformed'] = {'template': str(template_path)}
        return output_container