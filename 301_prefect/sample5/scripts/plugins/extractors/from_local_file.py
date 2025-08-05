# scripts/plugins/extractors/from_local_file.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class LocalFileExtractor(BaseExtractor):
    """
    Extracts data from a single file located on the local filesystem.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.path = Path(self.params.get("path"))
        self.format = self.params.get("format")
        self.encoding = self.params.get("encoding", "utf-8")
        self.pandas_options = self.params.get("pandas_options", {})
        if not self.path:
            raise ValueError("LocalFileExtractor requires a 'path' parameter.")

    def _infer_format(self) -> SupportedFormats:
        if self.format: return SupportedFormats.from_string(self.format)
        suffix = self.path.suffix.lower()
        if suffix == '.csv': return SupportedFormats.CSV
        elif suffix == '.json': return SupportedFormats.JSON
        elif suffix == '.parquet': return SupportedFormats.PARQUET
        else: raise ValueError(f"Could not infer file format from extension '{suffix}'.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        print(f"Extracting data from local file: {self.path}")
        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found at: {self.path}")

        file_format = self._infer_format()
        if file_format == SupportedFormats.CSV:
            df = pd.read_csv(self.path, encoding=self.encoding, **self.pandas_options)
        elif file_format == SupportedFormats.JSON:
            df = pd.read_json(self.path, encoding=self.encoding, **self.pandas_options)
        elif file_format == SupportedFormats.PARQUET:
            df = pd.read_parquet(self.path, **self.pandas_options)
        else:
            raise ValueError(f"Unsupported file format for LocalFileExtractor: {file_format.value}")

        container = DataContainer(data=df)
        container.metadata['source_path'] = str(self.path.resolve())
        container.metadata['source_format'] = file_format.value
        container.add_file_path(self.path)
        print(f"Successfully extracted {len(df)} rows into a DataFrame.")
        return container