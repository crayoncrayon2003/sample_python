# scripts/plugins/validators/ngsi_validator.py

import json
from typing import Dict, Any, List

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class NgsiValidator(BaseValidator):
    """
    Performs basic structural validation for NGSI entities (v2 or LD).

    This validator checks for the presence of mandatory NGSI attributes like
    'id' and 'type', and verifies the basic structure of attribute properties.
    It is not a full specification-compliant validator but serves as a
    good sanity check.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the NGSI validator.

        Expected params:
            - target_column (str): The name of the column containing the
              NGSI entity objects (can be JSON strings or dicts).
            - ngsi_version (str, optional): The NGSI version to check against.
              Supported: 'v2', 'ld'. Defaults to 'ld'.
            - stop_on_first_error (bool, optional): If True, stops on the
              first invalid entity. Defaults to True.
        """
        super().__init__(params)
        self.target_column = self.params.get("target_column")
        self.ngsi_version = self.params.get("ngsi_version", "ld").lower()
        self.stop_on_first_error = self.params.get("stop_on_first_error", True)

        if not self.target_column:
            raise ValueError("NgsiValidator requires a 'target_column' parameter.")
        if self.ngsi_version not in ['v2', 'ld']:
            raise ValueError("Unsupported 'ngsi_version'. Must be 'v2' or 'ld'.")

    def _validate_entity(self, entity: Dict[str, Any], index: int) -> List[str]:
        """Validates a single NGSI entity dictionary."""
        errors = []
        
        # Common checks for both v2 and LD
        if 'id' not in entity:
            errors.append(f"Row {index}: Entity is missing mandatory 'id' property.")
        if 'type' not in entity:
            errors.append(f"Row {index}: Entity is missing mandatory 'type' property.")

        # Check attributes (skip 'id', 'type', and '@context' for LD)
        reserved_keys = {'id', 'type'}
        if self.ngsi_version == 'ld':
            reserved_keys.add('@context')
        
        attributes = {k: v for k, v in entity.items() if k not in reserved_keys}
        for attr_name, attr_value in attributes.items():
            if not isinstance(attr_value, dict):
                errors.append(f"Row {index}: Attribute '{attr_name}' must be a dictionary object.")
                continue

            if self.ngsi_version == 'v2':
                if 'type' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' is missing 'type'.")
                if 'value' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' is missing 'value'.")
            
            elif self.ngsi_version == 'ld':
                if 'type' not in attr_value or attr_value['type'] not in ['Property', 'Relationship', 'GeoProperty']:
                    errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' has invalid or missing 'type'. Must be 'Property', 'Relationship', or 'GeoProperty'.")
                
                key_to_check = 'object' if attr_value.get('type') == 'Relationship' else 'value'
                if key_to_check not in attr_value:
                     errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' of type '{attr_value.get('type')}' is missing '{key_to_check}'.")
        
        return errors

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Validates each NGSI entity in the target column.
        """
        if data.data is None:
            print("Warning: NgsiValidator received a DataContainer with no DataFrame. Skipping.")
            return data
        
        df = data.data
        if self.target_column not in df.columns:
            raise KeyError(f"The target column '{self.target_column}' was not found in the DataFrame.")

        print(f"Validating NGSI-{self.ngsi_version} entities in column '{self.target_column}'.")
        all_errors: List[str] = []

        for index, record in df[self.target_column].items():
            instance = record
            if isinstance(record, str):
                try:
                    instance = json.loads(record)
                except json.JSONDecodeError:
                    error_msg = f"Row {index}: Invalid JSON string format."
                    if self.stop_on_first_error: raise ValueError(error_msg)
                    all_errors.append(error_msg)
                    continue
            
            if not isinstance(instance, dict):
                error_msg = f"Row {index}: Record is not a dictionary object."
                if self.stop_on_first_error: raise TypeError(error_msg)
                all_errors.append(error_msg)
                continue

            entity_errors = self._validate_entity(instance, index)
            if entity_errors:
                if self.stop_on_first_error:
                    raise ValueError("\n".join(entity_errors))
                all_errors.extend(entity_errors)

        if all_errors:
            summary = "\n- ".join(all_errors)
            raise ValueError(f"NGSI validation failed with {len(all_errors)} errors:\n- {summary}")

        print("NGSI validation successful. All entities appear well-formed.")
        return data