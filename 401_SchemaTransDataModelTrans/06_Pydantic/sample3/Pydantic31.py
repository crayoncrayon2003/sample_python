import json
import sys
import os
import csv
from pathlib import Path
from typing import Any, Dict, List
from datamodel_code_generator import generate, PythonVersion, InputFileType
import importlib.util

def load_csv_data(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

def load_json_schema(file_path: Path) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)

def load_generated_model(module_path: Path):
    spec = importlib.util.spec_from_file_location(module_path.stem, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    sys.modules[module_path.stem] = module
    return module

def main():
    directory = os.path.dirname(os.path.abspath(__file__))
    # -----------------------
    # 1. load schema
    # -----------------------
    input_schema_path = os.path.join(directory, "schema", "input_schema.json")
    output_schema_path = os.path.join(directory, "schema", "output_schema.json")

    input_schema = load_json_schema(Path(input_schema_path))
    output_schema = load_json_schema(Path(output_schema_path))

    # -----------------------
    # 2. Generate data model using datamodel-code-generator
    #    The data model is output to a file.
    # -----------------------
    output_dir = os.path.join(directory, "generated_models")
    os.makedirs(output_dir, exist_ok=True)

    input_model_file = os.path.join(output_dir, "input_model.py")
    output_model_file = os.path.join(output_dir, "output_model.py")

    # the input data model
    generate(
        input_=json.dumps(input_schema),
        input_file_type=InputFileType.JsonSchema,
        target_python_version=PythonVersion.PY_312,
        output=Path(input_model_file)
    )

    # the output data model
    generate(
        input_=json.dumps(output_schema),
        input_file_type=InputFileType.JsonSchema,
        target_python_version=PythonVersion.PY_312,
        output=Path(output_model_file)
    )

    # -----------------------
    # 3. Create an instance of the data model
    # -----------------------
    # load data model
    input_model_module = load_generated_model(Path(input_model_file))
    output_model_module = load_generated_model(Path(output_model_file))

    # Create an instance
    InputData = getattr(input_model_module, "Inputschema", None)
    OutputData = getattr(output_model_module, "Outputschema", None)

    if not InputData or not OutputData:
        raise ImportError("Model failed to load.")

    # -----------------------
    # 4. mapping
    # -----------------------
    def detect_ngsi_type(value: Any) -> str:
        if isinstance(value, str):
            return "Text"
        elif isinstance(value, bool):
            return "Boolean"
        elif isinstance(value, int):
            return "Integer"
        elif isinstance(value, float):
            return "Number"
        elif isinstance(value, list):
            return "Array"
        elif isinstance(value, dict):
            return "StructuredValue"
        else:
            return "Text"

    def convert_to_output(input_data: InputData, idx: int) -> OutputData:
        output_data = {}

        for field_name, field_value in input_data.model_dump().items():
            if field_name in OutputData.model_fields:
                output_data[field_name] = {
                    "value": field_value,
                    "type": detect_ngsi_type(field_value),
                }

        output_data["entityId"] = f"urn:ngsi-ld:csv:{idx+1}"
        output_data["entityType"] = "Place"

        return OutputData(**output_data)

    # -----------------------
    # 5. load csv file
    # -----------------------
    csv_path = os.path.join(directory, "data.csv")
    csv_data = load_csv_data(Path(csv_path))

    # -----------------------
    # 6. exec 1
    # -----------------------
    for idx, row in enumerate(csv_data):
        input_model = InputData(**row)
        output_model = convert_to_output(input_model, idx)
        print(output_model.model_dump_json(indent=2))

    # -----------------------
    # 7. exec 2
    # -----------------------
    output_list = []
    for idx, row in enumerate(csv_data):
        input_model = InputData(**row)
        output_model = convert_to_output(input_model, idx)
        output_list.append(output_model)

    output_json = [model.model_dump_json(indent=2) for model in output_list]
    print("[")
    print(",\n".join(output_json))
    print("]")

if __name__ == "__main__":
    main()

