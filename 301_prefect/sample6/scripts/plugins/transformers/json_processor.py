# scripts/plugins/transformers/json_processor.py

import json
import pandas as pd
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonProcessor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "json_processor"

    def _parse_json(self, json_string: str) -> Any:
        if not isinstance(json_string, str): return None
        try: return json.loads(json_string)
        except json.JSONDecodeError: return None

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        json_column = params.get("json_column")
        drop_invalid = params.get("drop_invalid", False)
        output_column_name = params.get("output_column_name") or json_column
        if not json_column: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'json_column'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: JsonProcessor received no DataFrame.")
            return data

        df = data.data.copy()
        if json_column not in df.columns:
            raise KeyError(f"Column '{json_column}' not found in DataFrame.")

        print(f"Processing JSON strings in column '{json_column}'.")
        parsed_series = df[json_column].apply(self._parse_json)
        invalid_count = parsed_series.isnull().sum()
        if invalid_count > 0: print(f"Found {invalid_count} invalid JSON rows.")
        if drop_invalid and invalid_count > 0:
            df = df[parsed_series.notna()].reset_index(drop=True)
            parsed_series = parsed_series.dropna().reset_index(drop=True)
            print("Dropped invalid JSON rows.")

        df[output_column_name] = parsed_series
        print(f"JSON processing complete. Final shape: {df.shape}")

        output_container = DataContainer(data=df)
        output_container.metadata = data.metadata.copy()
        output_container.metadata['json_processed'] = {'column': json_column, 'invalid_count': int(invalid_count), 'dropped_invalid': drop_invalid}
        return output_container