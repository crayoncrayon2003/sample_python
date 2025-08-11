# backend/plugins/transformers/json_processor.py

import json
import pandas as pd
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonProcessor:
    """
    (File-based) Reads a JSON Lines file, parses each line, and saves as Parquet.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "json_processor"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input JSON Lines Path",
                    "description": "The source JSON Lines (.jsonl) file."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Parquet Path",
                    "description": "Path to save the processed data as a Parquet file."
                },
                "drop_invalid": {
                    "type": "boolean",
                    "title": "Drop Invalid Rows",
                    "description": "If true, rows with invalid JSON will be dropped.",
                    "default": False
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        drop_invalid = params.get("drop_invalid", False)

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading JSON Lines file '{input_path}' for processing...")
        records = []
        invalid_count = 0
        with open(input_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    invalid_count += 1
                    if not drop_invalid:
                        records.append(None)

        if invalid_count > 0:
            print(f"Found {invalid_count} invalid JSON lines.")
            if drop_invalid:
                records = [r for r in records if r is not None]
                print("Dropped invalid JSON lines.")

        df = pd.DataFrame(records)
        print(f"Processing complete. Shape: {df.shape}. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container