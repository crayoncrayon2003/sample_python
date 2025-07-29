# scripts/tasks/validation/validate_json.py
import json
from pathlib import Path
from typing import Union

from jsonschema import validate, ValidationError
from prefect import task


@task
def validate_json_with_schema(
    data_path: Union[str, Path], schema_path: Union[str, Path]
) -> None:
    """
    Validates a JSON data file against a given JSON Schema.

    This task is a crucial step for ensuring data quality and integrity.
    It raises a `ValidationError` if the data does not conform to the schema,
    which will cause the Prefect task run to fail, stopping the pipeline.

    Args:
        data_path: The path to the JSON file to be validated.
        schema_path: The path to the JSON Schema file (.json).

    Raises:
        ValidationError: If the validation fails.
    """
    # Ensure paths are Path objects for consistent handling
    data_path = Path(data_path)
    schema_path = Path(schema_path)

    print(f"Validation: Checking '{data_path.name}' against schema '{schema_path.name}'...")

    # Load the schema and data from their respective files
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        print(f"Validation Error: File not found - {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Validation Error: Could not decode JSON from file - {e}")
        raise

    # Perform the validation
    try:
        validate(instance=data, schema=schema)
        print(f"Validation successful: '{data_path.name}' conforms to the schema.")
    except ValidationError as e:
        # Log a more detailed error message before re-raising
        print(f"Validation failed: '{data_path.name}' does not conform to the schema.")
        print(f"Reason: {e.message}")
        print(f"Path in data: {list(e.path)}")
        raise  # Re-raise the exception to fail the Prefect task