# scripts/plugins/extractors/from_local_file.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

hookimpl = pluggy.HookimplMarker("etl_framework")

class LocalFileExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_local_file"

    def _infer_format(self, path: Path, format_str: str | None) -> SupportedFormats:
        if format_str: return SupportedFormats.from_string(format_str)
        suffix = path.suffix.lower()
        if suffix == '.csv': return SupportedFormats.CSV
        elif suffix == '.json': return SupportedFormats.JSON
        elif suffix == '.parquet': return SupportedFormats.PARQUET
        else: raise ValueError(f"Could not infer file format from extension '{suffix}'.")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        path = Path(params.get("path"))
        format_str = params.get("format")
        encoding = params.get("encoding", "utf-8")
        pandas_options = params.get("pandas_options", {})
        if not path: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a 'path' parameter.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        print(f"Extracting data from local file: {path}")
        if not path.exists(): raise FileNotFoundError(f"Source file not found at: {path}")

        file_format = self._infer_format(path, format_str)
        if file_format == SupportedFormats.CSV:
            df = pd.read_csv(path, encoding=encoding, **pandas_options)
        elif file_format == SupportedFormats.JSON:
            df = pd.read_json(path, encoding=encoding, **pandas_options)
        elif file_format == SupportedFormats.PARQUET:
            df = pd.read_parquet(path, **pandas_options)
        else:
            raise ValueError(f"Unsupported file format for LocalFileExtractor: {file_format.value}")

        container = DataContainer(data=df)
        container.metadata['source_path'] = str(path.resolve())
        container.metadata['source_format'] = file_format.value
        container.add_file_path(path)
        print(f"Successfully extracted {len(df)} rows into a DataFrame.")
        return container