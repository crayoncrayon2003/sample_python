# scripts/plugins/validators/ngsi_validator.py

import json
from typing import Dict, Any, List, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NgsiValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "ngsi_validator"

    def _validate_entity(self, entity: Dict[str, Any], index: int) -> List[str]:
        errors = []
        if 'id' not in entity: errors.append(f"Row {index}: Missing 'id'.")
        if 'type' not in entity: errors.append(f"Row {index}: Missing 'type'.")
        reserved_keys = {'id', 'type', '@context'}
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

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        self.target_column = params.get("target_column")
        self.ngsi_version = params.get("ngsi_version", "ld").lower()
        stop_on_first_error = params.get("stop_on_first_error", True)
        if not self.target_column: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'target_column'.")
        if self.ngsi_version not in ['v2', 'ld']: raise ValueError("Unsupported 'ngsi_version'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: NgsiValidator received no DataFrame.")
            return data
        df = data.data
        if self.target_column not in df.columns: raise KeyError(f"Target column '{self.target_column}' not found.")
        print(f"Validating NGSI-{self.ngsi_version} entities in '{self.target_column}'.")
        all_errors: List[str] = []

        for index, record in df[self.target_column].items():
            instance = record
            if isinstance(record, str):
                try: instance = json.loads(record)
                except json.JSONDecodeError:
                    if stop_on_first_error: raise ValueError(f"Row {index}: Invalid JSON string.")
                    all_errors.append(f"Row {index}: Invalid JSON string."); continue
            if not isinstance(instance, dict):
                if stop_on_first_error: raise TypeError(f"Row {index}: Record not a dict.")
                all_errors.append(f"Row {index}: Record not a dict."); continue

            entity_errors = self._validate_entity(instance, index)
            if entity_errors:
                if stop_on_first_error: raise ValueError("\n".join(entity_errors))
                all_errors.extend(entity_errors)

        if all_errors:
            raise ValueError(f"NGSI validation failed:\n- " + "\n- ".join(all_errors))
        print("NGSI validation successful.")
        return data