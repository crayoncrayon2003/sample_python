# scripts/plugins/transformers/json_processor.py

import json
import pandas as pd
from typing import Dict, Any, List, Optional

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class JsonProcessor(BaseTransformer):
    """
    Processes a column of JSON strings within a DataFrame.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.json_column = self.params.get("json_column")
        self.drop_invalid = self.params.get("drop_invalid", False)
        self.output_column_name = self.params.get("output_column_name") or self.json_column
        if not self.json_column:
            raise ValueError("JsonProcessor requires a 'json_column' parameter.")

    def _parse_json(self, json_string: str) -> Any:
        if not isinstance(json_string, str): return None
        try: return json.loads(json_string)
        except json.JSONDecodeError: return None

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("JsonProcessor requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: JsonProcessor received a DataContainer with no DataFrame. Skipping.")
            return data

        df = data.data.copy()
        if self.json_column not in df.columns:
            raise KeyError(f"The specified 'json_column' ('{self.json_column}') was not found in the DataFrame.")

        print(f"Processing JSON strings in column '{self.json_column}'.")
        parsed_series = df[self.json_column].apply(self._parse_json)
        invalid_count = parsed_series.isnull().sum()
        if invalid_count > 0:
            print(f"Found {invalid_count} rows with invalid JSON.")

        if self.drop_invalid and invalid_count > 0:
            df = df[parsed_series.notna()].reset_index(drop=True)
            parsed_series = parsed_series.dropna().reset_index(drop=True)
            print("Dropped rows with invalid JSON.")

        df[self.output_column_name] = parsed_series
        print(f"JSON processing complete. Final shape: {df.shape}")

        output_container = DataContainer(data=df)
        output_container.metadata = data.metadata.copy()
        output_container.metadata['json_processed'] = {
            'column': self.json_column,
            'invalid_count': int(invalid_count),
            'dropped_invalid': self.drop_invalid
        }

        return output_container