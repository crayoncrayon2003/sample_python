# scripts/plugins/validators/ngsi_validator.py

import json
from typing import Dict, Any, List, Optional

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class NgsiValidator(BaseValidator):
    """
    Performs basic structural validation for NGSI entities (v2 or LD).
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.target_column = self.params.get("target_column")
        self.ngsi_version = self.params.get("ngsi_version", "ld").lower()
        self.stop_on_first_error = self.params.get("stop_on_first_error", True)
        if not self.target_column: raise ValueError("NgsiValidator requires a 'target_column'.")
        if self.ngsi_version not in ['v2', 'ld']: raise ValueError("Unsupported 'ngsi_version'.")

    def _validate_entity(self, entity: Dict[str, Any], index: int) -> List[str]:
        errors = []
        if 'id' not in entity: errors.append(f"Row {index}: Missing 'id'.")
        if 'type' not in entity: errors.append(f"Row {index}: Missing 'type'.")
        reserved_keys = {'id', 'type'}
        if self.ngsi_version == 'ld': reserved_keys.add('@context')
        attributes = {k: v for k, v in entity.items() if k not in reserved_keys}
        for attr_name, attr_value in attributes.items():
            if not isinstance(attr_value, dict): errors.append(f"Row {index}: Attr '{attr_name}' must be dict."); continue
            if self.ngsi_version == 'v2':
                if 'type' not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing 'type'.")
                if 'value' not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing 'value'.")
            elif self.ngsi_version == 'ld':
                if attr_value.get('type') not in ['Property', 'Relationship', 'GeoProperty']: errors.append(f"Row {index}: Attr '{attr_name}' invalid 'type'.")
                key = 'object' if attr_value.get('type') == 'Relationship' else 'value'
                if key not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing '{key}'.")
        return errors

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("NgsiValidator requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: NgsiValidator received no DataFrame. Skipping.")
            return data
        df = data.data
        if self.target_column not in df.columns:
            raise KeyError(f"Target column '{self.target_column}' not found.")
        print(f"Validating NGSI-{self.ngsi_version} entities in '{self.target_column}'.")
        all_errors: List[str] = []

        for index, record in df[self.target_column].items():
            instance = record
            if isinstance(record, str):
                try: instance = json.loads(record)
                except json.JSONDecodeError:
                    if self.stop_on_first_error: raise ValueError(f"Row {index}: Invalid JSON string.")
                    all_errors.append(f"Row {index}: Invalid JSON string."); continue
            if not isinstance(instance, dict):
                if self.stop_on_first_error: raise TypeError(f"Row {index}: Record not a dict.")
                all_errors.append(f"Row {index}: Record not a dict."); continue

            entity_errors = self._validate_entity(instance, index)
            if entity_errors:
                if self.stop_on_first_error: raise ValueError("\n".join(entity_errors))
                all_errors.extend(entity_errors)

        if all_errors:
            raise ValueError(f"NGSI validation failed:\n- " + "\n- ".join(all_errors))
        print("NGSI validation successful.")
        return data