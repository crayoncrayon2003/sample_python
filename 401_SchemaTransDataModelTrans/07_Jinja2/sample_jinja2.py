import json
import os
import csv
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

def load_csv_data(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

def load_json_schema(file_path: Path) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)

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

def main():
    directory = os.path.dirname(os.path.abspath(__file__))
    # -----------------------
    # 1. load schema
    # -----------------------
    input_schema_path = os.path.join(directory, "schema", "input_schema.json")
    output_schema_path = os.path.join(directory, "schema", "output_schema.json")

    # -----------------------
    # 2. Generate data model using datamodel-code-generator
    #    The data model is output to a file.
    # -----------------------
    output_dir = os.path.join(directory, "generated_models")
    os.makedirs(output_dir, exist_ok=True)

    input_schema = load_json_schema(Path(input_schema_path))
    output_schema = load_json_schema(Path(output_schema_path))

    # -----------------------
    # 3. setting Jinja2 env
    # -----------------------
    env = Environment(
        loader=FileSystemLoader(searchpath=os.path.join(directory, "templates")),
        autoescape=select_autoescape(["j2"]),
        trim_blocks=True,
        lstrip_blocks=True
    )
    template = env.get_template("ngsi_template.j2")

    # -----------------------
    # 4. load csv file
    # -----------------------
    csv_path = os.path.join(directory, "data.csv")
    csv_data = load_csv_data(Path(csv_path))

    # 5. exec
    output_json = template.render(
        data=csv_data,
        output_schema=output_schema,
        detect_ngsi_type=detect_ngsi_type
    )

    print(json.dumps(json.loads(output_json), ensure_ascii=False, indent=4))



if __name__ == "__main__":
    main()
