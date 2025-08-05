# scripts/plugins/extractors/from_local_json.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class LocalJsonExtractor(BaseExtractor):
    """
    Extracts data from a JSON file located on the local filesystem.

    This extractor is specifically designed for JSON files and provides
    options to handle nested structures using pandas' `json_normalize`.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with parameters for JSON file loading.

        Expected params:
            - path (str): The absolute or relative path to the source JSON file.
            - encoding (str, optional): The file encoding. Defaults to 'utf-8'.
            - pandas_options (dict, optional): A dictionary of additional options
              to pass directly to the `pd.read_json` or `pd.json_normalize` function.
              Example: `orient: 'records'`, `lines: True`.
            - normalize_options (dict, optional): A dictionary of options for
              handling nested JSON. If present, `pd.json_normalize` is used.
              Example: `record_path: ['items']`, `meta: ['requestId']`.
        """
        super().__init__(params)
        self.path = Path(self.params.get("path"))
        self.encoding = self.params.get("encoding", "utf-8")
        self.pandas_options = self.params.get("pandas_options", {})
        self.normalize_options: Optional[Dict[str, Any]] = self.params.get("normalize_options")

        if not self.path:
            raise ValueError("LocalJsonExtractor requires a 'path' parameter.")

    def execute(self) -> DataContainer:
        """
        Reads the specified local JSON file and loads its content into a DataFrame.
        If `normalize_options` are provided, it will attempt to flatten a
        nested JSON structure.

        Returns:
            DataContainer: A new container holding the JSON data and metadata.
        
        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        print(f"Extracting data from local JSON file: {self.path}")

        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found at: {self.path}")

        df: pd.DataFrame
        
        if self.normalize_options:
            # Handle nested JSON using json_normalize
            import json
            print(f"Applying JSON normalization with options: {self.normalize_options}")
            
            with self.path.open('r', encoding=self.encoding) as f:
                json_data = json.load(f)
            
            # Combine normalize options with other pandas options
            # User might want to specify `meta_prefix` etc.
            combined_options = {**self.pandas_options, **self.normalize_options}
            df = pd.json_normalize(json_data, **combined_options)

        else:
            # Handle "flat" JSON files using read_json
            df = pd.read_json(self.path, encoding=self.encoding, **self.pandas_options)

        container = DataContainer(data=df)
        container.metadata['source_path'] = str(self.path.resolve())
        container.metadata['source_format'] = 'json'
        container.add_file_path(self.path)

        print(f"Successfully extracted {len(df)} rows from JSON into a DataFrame.")
        return container