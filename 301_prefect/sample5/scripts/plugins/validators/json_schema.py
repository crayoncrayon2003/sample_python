# scripts/plugins/validators/json_schema.py

import json
from pathlib import Path
from typing import Dict, Any, List, Optional

import jsonschema
from jsonschema import Draft7Validator
import pandas as pd

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class JsonSchemaValidator(BaseValidator):
    """
    Validates a column of JSON objects against a specified JSON Schema.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.schema_path = Path(self.params.get("schema_path"))
        self.target_column = self.params.get("target_column")
        self.stop_on_first_error = self.params.get("stop_on_first_error", True)

        if not self.schema_path or not self.target_column:
            raise ValueError("JsonSchemaValidator requires 'schema_path' and 'target_column' parameters.")
        if not self.schema_path.exists():
            raise FileNotFoundError(f"JSON Schema file not found at: {self.schema_path}")

        try:
            with self.schema_path.open('r', encoding='utf-8') as f:
                self.schema = json.load(f)
            Draft7Validator.check_schema(self.schema)
            self.validator = Draft7Validator(self.schema)
        except (json.JSONDecodeError, jsonschema.SchemaError) as e:
            raise ValueError(f"Invalid JSON Schema file '{self.schema_path}': {e}")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("JsonSchemaValidator requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: JsonSchemaValidator received a DataContainer with no DataFrame. Skipping.")
            return data
        
        df = data.data
        if self.target_column not in df.columns:
            raise KeyError(f"The target column '{self.target_column}' was not found in the DataFrame.")

        print(f"Validating column '{self.target_column}' against schema '{self.schema_path.name}'.")
        
        errors: List[str] = []
        
        for index, record in df[self.target_column].items():
            instance = record
            if isinstance(record, str):
                try: instance = json.loads(record)
                except json.JSONDecodeError:
                    error_msg = f"Row {index}: Invalid JSON string format."
                    if self.stop_on_first_error: raise ValueError(error_msg)
                    errors.append(error_msg)
                    continue
            
            validation_errors = sorted(self.validator.iter_errors(instance), key=lambda e: e.path)
            if validation_errors:
                error_msg = f"Row {index}: Validation failed for record: {instance}\n"
                for error in validation_errors:
                    error_msg += f"  - Path: '{'/'.join(map(str, error.path))}', Rule: '{error.validator}', Message: {error.message}\n"
                
                if self.stop_on_first_error: raise jsonschema.ValidationError(error_msg)
                errors.append(error_msg)

        if errors:
            all_errors = "\n".join(errors)
            raise ValueError(f"JSON Schema validation failed for {len(errors)} records:\n{all_errors}")

        print("JSON Schema validation successful. All records conform to the schema.")
        return data