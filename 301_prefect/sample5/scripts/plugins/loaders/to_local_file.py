# scripts/plugins/loaders/to_local_file.py

from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from .base import BaseLoader
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class LocalFileLoader(BaseLoader):
    """
    Loads data from a DataFrame into a file on the local filesystem.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.output_path = Path(self.params.get("output_path"))
        self.format = SupportedFormats.from_string(self.params.get("format", "csv"))
        self.pandas_options = self.params.get("pandas_options", {})
        if not self.output_path:
            raise ValueError("LocalFileLoader requires an 'output_path' parameter.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> None:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("LocalFileLoader requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: LocalFileLoader received a DataContainer with no DataFrame to save. Skipping.")
            return

        df = data.data
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Loading {len(df)} rows to local file: {self.output_path} (Format: {self.format.value})")

        try:
            if self.format == SupportedFormats.CSV:
                if 'index' not in self.pandas_options:
                    self.pandas_options['index'] = False
                df.to_csv(self.output_path, **self.pandas_options)
            elif self.format == SupportedFormats.JSON:
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