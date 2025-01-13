import os
import re
import json
from jsonschema import validate, ValidationError


class DataModelTransformer:
    def __init__(self, source_schema_path, target_schema_path, mapping_path):
        self.source_schema = self._load_json(source_schema_path)
        self.target_schema = self._load_json(target_schema_path)
        self.mapping = self._load_json(mapping_path)

    def _load_json(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File '{path}' not found.")
        try:
            with open(path, "r") as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in '{path}': {e}")

    def _process_mapping(self, source_data, mapping):
        mapped_data = {}
        for field, rules in mapping.items():
            source = rules.get("source")
            target = rules.get("target")
            default = rules.get("default")
            value = source_data.get(source) if source else default

            # 値がどちらも None の場合スキップ
            if value is None:
                continue

            # ターゲットに値を配置
            keys = target.split(".")
            current = mapped_data
            for key in keys[:-1]:
                if key not in current or not isinstance(current[key], dict):
                    current[key] = {}
                current = current[key]
            current[keys[-1]] = value

            # name, temperature, humidity に追加のプロパティを補完
            if field in ["name", "temperature", "humidity"]:
                current["type"] = rules.get("type", "defaultType")
                current["metadata"] = {}

        print("Mapped data:", json.dumps(mapped_data, indent=2))
        return mapped_data

    def validation(self, data, schema):
        try:
            validate(instance=data, schema=schema)
            print("Validation successful.")
            return True
        except ValidationError as e:
            print(f"Validation error: {e.message}")
            return False

    def transform(self, source_data):
        # Validate source data
        if not self.validation(source_data, self.source_schema):
            raise ValueError("Source data validation failed.")

        # Apply mapping to generate target data
        target_data = self._process_mapping(source_data, self.mapping)

        # デバッグ: ターゲットデータ検証前に内容確認
        print("Pre-validation target data:", json.dumps(target_data, indent=2))

        # Validate target data
        if not self.validation(target_data, self.target_schema):
            raise ValueError("Target data validation failed.")

        return target_data



def main():
    root = os.path.dirname(os.path.abspath(__file__))
    source_schema_path = os.path.join(root, "json_schema.json")
    target_schema_path = os.path.join(root, "ngsi_schema.json")
    mapping_path = os.path.join(root, "mapping.json")

    source_data = {
        "name": "Sample Entity",
        "temperature": 20,
        "humidity": 50
    }

    try:
        transformer = DataModelTransformer(source_schema_path, target_schema_path, mapping_path)

        # Data Model Transformer
        target_data = transformer.transform(source_data)

        print("Source data:")
        print(json.dumps(source_data, indent=2))
        print("Target data:")
        print(json.dumps(target_data, indent=2))

    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
