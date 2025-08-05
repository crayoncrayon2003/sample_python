# scripts/plugins/validators/json_schema.py

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import jsonschema
from jsonschema import Draft7Validator
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonSchemaValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "json_schema"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        schema_path = Path(params.get("schema_path"))
        target_column = params.get("target_column")
        stop_on_first_error = params.get("stop_on_first_error", True)
        if not schema_path or not target_column:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'schema_path' and 'target_column'.")
        if not schema_path.exists(): raise FileNotFoundError(f"JSON Schema not found: {schema_path}")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: JsonSchemaValidator received no DataFrame.")
            return data

        try:
            with schema_path.open('r', encoding='utf-8') as f: schema = json.load(f)
            Draft7Validator.check_schema(schema)
            validator = Draft7Validator(schema)
        except (json.JSONDecodeError, jsonschema.SchemaError) as e:
            raise ValueError(f"Invalid JSON Schema file '{schema_path}': {e}")

        df = data.data
        if target_column not in df.columns: raise KeyError(f"Target column '{target_column}' not found.")
        print(f"Validating column '{target_column}' against schema '{schema_path.name}'.")
        errors: List[str] = []

        for index, record in df[target_column].items():
            instance = record
            if isinstance(record, str):
                try: instance = json.loads(record)
                except json.JSONDecodeError:
                    error_msg = f"Row {index}: Invalid JSON string format."
                    if stop_on_first_error: raise ValueError(error_msg)
                    errors.append(error_msg); continue

            validation_errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
            if validation_errors:
                error_msg = f"Row {index}: Validation failed.\n"
                for error in validation_errors: error_msg += f"  - Path: '{'/'.join(map(str, error.path))}', Rule: '{error.validator}', Msg: {error.message}\n"
                if stop_on_first_error: raise jsonschema.ValidationError(error_msg)
                errors.append(error_msg)

        if errors:
            raise ValueError(f"JSON Schema validation failed for {len(errors)} records:\n" + "\n".join(errors))
        print("JSON Schema validation successful.")
        return data