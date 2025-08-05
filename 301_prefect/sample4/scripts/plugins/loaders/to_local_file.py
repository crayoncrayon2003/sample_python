# scripts/plugins/loaders/to_local_file.py

from pathlib import Path
from typing import Dict, Any

import pandas as pd
from .base import BaseLoader
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class LocalFileLoader(BaseLoader):
    """
    Loads data from a DataFrame into a file on the local filesystem.

    This loader takes the DataFrame from the input DataContainer and writes
    it to a specified file path in a given format (e.g., CSV, JSON, Parquet).
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with file output parameters.

        Expected params:
            - output_path (str): The full path for the output file. The directory
              will be created if it doesn't exist.
            - format (str): The output format ('csv', 'json', 'parquet').
            - pandas_options (dict, optional): A dictionary of options to pass
              to the pandas `to_...` function (e.g., `to_csv`).
              Example for CSV: `{"index": False, "sep": ","}`.
        """
        super().__init__(params)
        self.output_path = Path(self.params.get("output_path"))
        self.format = SupportedFormats.from_string(self.params.get("format", "csv"))
        self.pandas_options = self.params.get("pandas_options", {})

        if not self.output_path:
            raise ValueError("LocalFileLoader requires an 'output_path' parameter.")

    def execute(self, data: DataContainer) -> None:
        """
        Writes the DataFrame to the specified local file.

        Args:
            data (DataContainer): The container with the DataFrame to save.
        """
        if data.data is None:
            print("Warning: LocalFileLoader received a DataContainer with no DataFrame to save. Skipping.")
            return

        df = data.data
        
        # Ensure the output directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Loading {len(df)} rows to local file: {self.output_path} (Format: {self.format.value})")

        try:
            if self.format == SupportedFormats.CSV:
                # Default index=False is a common requirement for CSV output
                if 'index' not in self.pandas_options:
                    self.pandas_options['index'] = False
                df.to_csv(self.output_path, **self.pandas_options)
            
            elif self.format == SupportedFormats.JSON:
                # Default orient='records' and lines=True is common for JSONL
                if 'orient' not in self.pandas_options:
                    self.pandas_options['orient'] = 'records'
                df.to_json(self.output_path, **self.pandas_options)
            
            elif self.format == SupportedFormats.PARQUET:
                if 'index' not in self.pandas_options:
                    self.pandas_options['index'] = False
                df.to_parquet(self.output_path, **self.pandas_options)
            
            else:
                raise ValueError(f"Unsupported format '{self.format.value}' for LocalFileLoader.")

        except Exception as e:
            print(f"ERROR saving file to {self.output_path}: {e}")
            raise
            
        print("File saved successfully.")
        # As a loader, this method returns nothing.