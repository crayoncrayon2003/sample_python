# scripts/plugins/validators/json_schema.py

import json
from pathlib import Path
from typing import Dict, Any, List

import jsonschema
from jsonschema import Draft7Validator
import pandas as pd

from .base import BaseValidator
from scripts.core.data_container.container import DataContainer

class JsonSchemaValidator(BaseValidator):
    """
    Validates a column of JSON objects against a specified JSON Schema.

    This validator checks each entry in a DataFrame column (which should contain
    either JSON strings or dictionary objects) to ensure it conforms to the
    structure defined in a JSON Schema file.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the validator with a schema and target column.

        Expected params:
            - schema_path (str): The file path to the JSON Schema (.json file).
            - target_column (str): The name of the column in the DataFrame to validate.
            - stop_on_first_error (bool, optional): If True, the validation process
              stops and raises an exception on the first invalid record. If False,
              it checks all records and reports all errors at the end.
              Defaults to True.
        """
        super().__init__(params)
        self.schema_path = Path(self.params.get("schema_path"))
        self.target_column = self.params.get("target_column")
        self.stop_on_first_error = self.params.get("stop_on_first_error", True)

        if not self.schema_path or not self.target_column:
            raise ValueError("JsonSchemaValidator requires 'schema_path' and 'target_column' parameters.")
        if not self.schema_path.exists():
            raise FileNotFoundError(f"JSON Schema file not found at: {self.schema_path}")

        # Load and parse the schema during initialization
        try:
            with self.schema_path.open('r', encoding='utf-8') as f:
                self.schema = json.load(f)
            # Check if the schema itself is a valid JSON Schema
            Draft7Validator.check_schema(self.schema)
            self.validator = Draft7Validator(self.schema)
        except (json.JSONDecodeError, jsonschema.SchemaError) as e:
            raise ValueError(f"Invalid JSON Schema file '{self.schema_path}': {e}")


    def execute(self, data: DataContainer) -> DataContainer:
        """
        Validates each JSON object in the target column against the schema.

        Args:
            data (DataContainer): The input container with the DataFrame to validate.

        Returns:
            DataContainer: The original container, if validation is successful.
        
        Raises:
            jsonschema.ValidationError: If `stop_on_first_error` is True and an
                                        invalid record is found.
            ValueError: If `stop_on_first_error` is False and any invalid
                        records are found.
        """
        if data.data is None:
            print("Warning: JsonSchemaValidator received a DataContainer with no DataFrame. Skipping.")
            return data
        
        df = data.data
        if self.target_column not in df.columns:
            raise KeyError(f"The target column '{self.target_column}' was not found in the DataFrame.")

        print(f"Validating column '{self.target_column}' against schema '{self.schema_path.name}'.")
        
        errors: List[str] = []
        
        # Iterate over each record in the target column
        for index, record in df[self.target_column].items():
            # If the record is a string, parse it to a dictionary first
            instance = record
            if isinstance(record, str):
                try:
                    instance = json.loads(record)
                except json.JSONDecodeError:
                    error_msg = f"Row {index}: Invalid JSON string format."
                    if self.stop_on_first_error:
                        raise ValueError(error_msg)
                    errors.append(error_msg)
                    continue # Move to the next record
            
            # Perform the validation
            validation_errors = sorted(self.validator.iter_errors(instance), key=lambda e: e.path)
            if validation_errors:
                error_msg = f"Row {index}: Validation failed for record: {instance}\n"
                for error in validation_errors:
                    error_msg += f"  - Path: '{'/'.join(map(str, error.path))}', Rule: '{error.validator}', Message: {error.message}\n"
                
                if self.stop_on_first_error:
                    # Raise the first detailed error found
                    raise jsonschema.ValidationError(error_msg)
                
                errors.append(error_msg)

        if errors:
            # If not stopping on first error, raise a single exception with all findings
            all_errors = "\n".join(errors)
            raise ValueError(f"JSON Schema validation failed for {len(errors)} records:\n{all_errors}")

        print("JSON Schema validation successful. All records conform to the schema.")
        # If we reach here, validation passed. Return the original container.
        return data