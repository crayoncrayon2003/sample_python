# scripts/plugins/transformers/to_ngsi.py

from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import uuid

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class ToNxsiTransformer(BaseTransformer):
    """
    Transforms DataFrame rows into NGSI entities (v2 or LD) using a Jinja2 template.

    This is a specialized version of the Jinja2Transformer, designed specifically
    for creating NGSI-compliant JSON objects. It simplifies the configuration
    by providing NGSI-specific parameters.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the transformer with a Jinja2 template and NGSI details.

        Expected params:
            - template_path (str): The path to the Jinja2 template file for the entity.
            - entity_type (str): The NGSI entity type (e.g., 'Device', 'WeatherObserved').
            - id_prefix (str, optional): A prefix to add to the entity ID.
              e.g., 'urn:ngsi-ld:WeatherObserved:'.
            - id_column (str, optional): The name of the column in the DataFrame
              that contains the unique ID for each entity. If not provided, a UUID
              will be generated for each entity.
            - output_column_name (str, optional): The name for the output column.
              Defaults to 'ngsi_entity'.
        """
        super().__init__(params)
        self.template_path = Path(self.params.get("template_path"))
        self.entity_type = self.params.get("entity_type")
        self.id_prefix = self.params.get("id_prefix", "")
        self.id_column = self.params.get("id_column")
        self.output_column_name = self.params.get("output_column_name", "ngsi_entity")

        if not self.template_path or not self.entity_type:
            raise ValueError("ToNxsiTransformer requires 'template_path' and 'entity_type' parameters.")
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found at: {self.template_path}")

        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_path.parent)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.template: Template = self.env.get_template(self.template_path.name)

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Renders the NGSI entity template for each row in the DataFrame.

        Args:
            data (DataContainer): The input container with the DataFrame.

        Returns:
            DataContainer: A new container with a single-column DataFrame
                           containing the rendered NGSI entities.
        """
        if data.data is None:
            print("Warning: ToNxsiTransformer received a DataContainer with no DataFrame. Skipping.")
            return data

        source_df = data.data
        print(f"Transforming {len(source_df)} rows into '{self.entity_type}' NGSI entities.")
        
        records = source_df.to_dict(orient='records')
        rendered_entities: List[str] = []

        for record in records:
            # Prepare context for the template
            context = record.copy()
            
            # Add NGSI-specific variables to the context so they can be used in the template
            context['entity_type'] = self.entity_type
            
            # Construct the entity ID
            if self.id_column:
                if self.id_column not in record:
                    raise KeyError(f"The specified id_column '{self.id_column}' was not found in the data.")
                entity_id_part = record[self.id_column]
            else:
                entity_id_part = uuid.uuid4() # Generate a new UUID if no ID column
            
            context['entity_id'] = f"{self.id_prefix}{entity_id_part}"

            try:
                # Render the template with the enriched context
                rendered_string = self.template.render(context)
                rendered_entities.append(rendered_string)
            except Exception as e:
                print(f"ERROR rendering NGSI template for record: {record}")
                print(f"Error was: {e}")
                rendered_entities.append(f'{{"error": "Failed to render NGSI entity: {e}"}}')

        # Create the output DataFrame
        result_df = pd.DataFrame(
            rendered_entities,
            columns=[self.output_column_name]
        )

        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.metadata['ngsi_transformed'] = {
            'entity_type': self.entity_type,
            'count': len(result_df)
        }

        return output_container