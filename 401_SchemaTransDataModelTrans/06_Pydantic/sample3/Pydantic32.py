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

    # the input data model
    generate(
        input_=json.dumps(input_schema),
        input_file_type=InputFileType.JsonSchema,
        target_python_version=PythonVersion.PY_312,
        output=Path(input_model_file)
    )

    # -----------------------
    # 3. Create an instance of the data model
    # -----------------------
    # load data model
    input_model_module = load_generated_model(Path(input_model_file))

    # Create an instance
    InputData = getattr(input_model_module, "Inputschema", None)

    if not InputData:
        raise ImportError("Inputschema クラスが見つかりません")

    # -----------------------
    # 4. carete UnifiedModel Class
    # -----------------------
    class UnifiedModel(InputData):
        def __init__(self, **kwargs):
            # kwargs で入力データを初期化
            super().__init__(**kwargs)

        def model_dump_json(self, *, indent: int = None, idx: int = 1, output_schema: Dict[str, Any] = None) -> str:
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

            data_dict = self.model_dump()

            if output_schema is None:
                ret = json.dumps(data_dict, indent=indent, ensure_ascii=False)
            else :
                output = {
                    "entityId": f"urn:ngsi-ld:csv:{idx}",
                    "entityType": "Place",
                }
                for field_name, field_info in output_schema.get("properties", {}).items():
                    if field_name in data_dict:
                        value = data_dict[field_name]
                        output[field_name] = {
                            "value": value,
                            "type": detect_ngsi_type(value)
                        }

                ret = json.dumps(output, indent=indent, ensure_ascii=False)

            return ret

    # -----------------------
    # 5. load csv file
    # -----------------------
    csv_path = os.path.join(directory, "data.csv")
    csv_data = load_csv_data(Path(csv_path))

    # -----------------------
    # 6. exec 1
    # -----------------------
    print("== 単発出力 ==")
    for idx, row in enumerate(csv_data):
        model = UnifiedModel(**row)
        print(model.model_dump_json(indent=2, idx=idx + 1, output_schema=output_schema))

    # -----------------------
    # 7. exec 2
    # -----------------------
    output_json_list = [
        UnifiedModel(**row).model_dump_json(indent=2, idx=idx + 1, output_schema=output_schema)
        for idx, row in enumerate(csv_data)
    ]
    print("[")
    print(",\n".join(output_json_list))
    print("]")


if __name__ == "__main__":
    main()

