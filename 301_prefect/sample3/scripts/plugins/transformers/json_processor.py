# scripts/plugins/transformers/json_processor.py

import json
import pandas as pd
from typing import Dict, Any, List

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class JsonProcessor(BaseTransformer):
    """
    Processes a column of JSON strings within a DataFrame.

    This transformer is typically used after a Jinja2Transformer to parse the
    rendered JSON strings into Python dictionary objects. It can validate
    the JSON and optionally filter out rows that contain invalid JSON.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the processor with JSON processing parameters.

        Expected params:
            - json_column (str): The name of the column containing the JSON strings.
            - drop_invalid (bool, optional): If True, rows with invalid JSON
              will be dropped. If False, they will be kept but the parsed
              value will be None. Defaults to False.
            - output_column_name (str, optional): The name of the new column that
              will hold the parsed dictionary objects. If not provided, the
              original `json_column` will be overwritten.
        """
        super().__init__(params)
        self.json_column = self.params.get("json_column")
        self.drop_invalid = self.params.get("drop_invalid", False)
        self.output_column_name = self.params.get("output_column_name") or self.json_column

        if not self.json_column:
            raise ValueError("JsonProcessor requires a 'json_column' parameter.")

    def _parse_json(self, json_string: str) -> Any:
        """
        Safely parses a single JSON string into a Python object.
        Returns None if parsing fails.
        """
        if not isinstance(json_string, str):
            return None # Handle non-string inputs gracefully
        try:
            return json.loads(json_string)
        except json.JSONDecodeError:
            return None

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Parses the specified JSON column into dictionary objects.

        Args:
            data (DataContainer): The input container with the DataFrame to process.

        Returns:
            DataContainer: A new container with the processed DataFrame.
        """
        if data.data is None:
            print("Warning: JsonProcessor received a DataContainer with no DataFrame. Skipping.")
            return data
        
        df = data.data.copy()

        if self.json_column not in df.columns:
            raise KeyError(f"The specified 'json_column' ('{self.json_column}') was not found in the DataFrame.")

        print(f"Processing JSON strings in column '{self.json_column}'.")
        
        # Apply the parsing function to each element in the specified column
        parsed_series = df[self.json_column].apply(self._parse_json)
        
        # Count invalid rows for logging
        invalid_count = parsed_series.isnull().sum()
        if invalid_count > 0:
            print(f"Found {invalid_count} rows with invalid JSON.")

        if self.drop_invalid and invalid_count > 0:
            # Drop rows where parsing resulted in None
            df = df[parsed_series.notna()].reset_index(drop=True)
            # Update the parsed series to align with the dropped rows
            parsed_series = parsed_series.dropna().reset_index(drop=True)
            print("Dropped rows with invalid JSON.")

        # Assign the parsed objects to the output column
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