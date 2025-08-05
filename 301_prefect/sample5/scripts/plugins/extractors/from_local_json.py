# scripts/plugins/extractors/from_local_json.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class LocalJsonExtractor(BaseExtractor):
    """
    Extracts data from a JSON file located on the local filesystem.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.path = Path(self.params.get("path"))
        self.encoding = self.params.get("encoding", "utf-8")
        self.pandas_options = self.params.get("pandas_options", {})
        self.normalize_options: Optional[Dict[str, Any]] = self.params.get("normalize_options")
        if not self.path:
            raise ValueError("LocalJsonExtractor requires a 'path' parameter.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        print(f"Extracting data from local JSON file: {self.path}")
        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found at: {self.path}")

        if self.normalize_options:
            import json
            print(f"Applying JSON normalization with options: {self.normalize_options}")
            with self.path.open('r', encoding=self.encoding) as f:
                json_data = json.load(f)
            combined_options = {**self.pandas_options, **self.normalize_options}
            df = pd.json_normalize(json_data, **combined_options)
        else:
            df = pd.read_json(self.path, encoding=self.encoding, **self.pandas_options)

        container = DataContainer(data=df)
        container.metadata['source_path'] = str(self.path.resolve())
        container.metadata['source_format'] = 'json'
        container.add_file_path(self.path)
        print(f"Successfully extracted {len(df)} rows from JSON into a DataFrame.")
        return container