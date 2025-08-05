# scripts/plugins/transformers/to_ngsi.py

from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import uuid
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ToNxsiTransformer:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_ngsi"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        template_path = Path(params.get("template_path"))
        entity_type = params.get("entity_type")
        id_prefix = params.get("id_prefix", "")
        id_column = params.get("id_column")
        output_column_name = params.get("output_column_name", "ngsi_entity")
        if not template_path or not entity_type:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'template_path' and 'entity_type'.")
        if not template_path.exists(): raise FileNotFoundError(f"Template not found: {template_path}")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: ToNxsiTransformer received no DataFrame.")
            return data

        env = Environment(loader=FileSystemLoader(str(template_path.parent)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_path.name)

        source_df = data.data
        print(f"Transforming {len(source_df)} rows into '{entity_type}' NGSI entities.")
        records = source_df.to_dict(orient='records')
        rendered_entities: List[str] = []

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
                rendered_entities.append(template.render(context))
            except Exception as e:
                print(f"ERROR rendering NGSI template for record: {record}. Error: {e}")
                rendered_entities.append(f'{{"error": "Failed to render NGSI entity: {e}"}}')

        result_df = pd.DataFrame(rendered_entities, columns=[output_column_name])
        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.metadata['ngsi_transformed'] = {'entity_type': entity_type, 'count': len(result_df)}
        return output_container