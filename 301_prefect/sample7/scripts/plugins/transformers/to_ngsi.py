# scripts/plugins/transformers/to_ngsi.py

from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import uuid
import json
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ToNxsiTransformer:
    """
    (File-based) Transforms rows from a file into NGSI entities using a Jinja2 template.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_ngsi"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        template_path = Path(params.get("template_path"))
        entity_type = params.get("entity_type")
        id_prefix = params.get("id_prefix", "")
        id_column = params.get("id_column")

        if not all([input_path, output_path, template_path, entity_type]):
            raise ValueError("Plugin requires 'input_path', 'output_path', 'template_path', and 'entity_type'.")
        if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
        if not template_path.exists(): raise FileNotFoundError(f"Template file not found: {template_path}")

        print(f"Reading file '{input_path}' to transform into NGSI entities...")
        df = pd.read_parquet(input_path)

        env = Environment(loader=FileSystemLoader(str(template_path.parent)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_path.name)

        print(f"Transforming {len(df)} rows into '{entity_type}' entities, writing to '{output_path}'...")
        records = df.to_dict(orient='records')

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            for record in records:
                context = record.copy()
                context['entity_type'] = entity_type
                if id_column:
                    if id_column not in record: raise KeyError(f"ID column '{id_column}' not found.")
                    entity_id_part = record[id_column]
                else:
                    entity_id_part = uuid.uuid4()
                context['entity_id'] = f"{id_prefix}{entity_id_part}"

                try:
                    rendered_string = template.render(context)
                    json_object = json.loads(rendered_string)
                    f.write(json.dumps(json_object) + '\n')
                except Exception as e:
                    print(f"ERROR rendering NGSI template for record: {record}. Error: {e}")
                    f.write(json.dumps({"error": str(e), "source_record": record}) + '\n')

        print("NGSI transformation complete.")
        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container