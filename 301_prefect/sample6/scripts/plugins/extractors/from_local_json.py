# scripts/plugins/extractors/from_local_json.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import json

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class LocalJsonExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_local_json"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        path = Path(params.get("path"))
        encoding = params.get("encoding", "utf-8")
        pandas_options = params.get("pandas_options", {})
        normalize_options: Optional[Dict[str, Any]] = params.get("normalize_options")
        if not path: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a 'path' parameter.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        print(f"Extracting data from local JSON file: {path}")
        if not path.exists(): raise FileNotFoundError(f"Source file not found at: {path}")

        if normalize_options:
            print(f"Applying JSON normalization with options: {normalize_options}")
            with path.open('r', encoding=encoding) as f:
                json_data = json.load(f)
            combined_options = {**pandas_options, **normalize_options}
            df = pd.json_normalize(json_data, **combined_options)
        else:
            df = pd.read_json(path, encoding=encoding, **pandas_options)

        container = DataContainer(data=df)
        container.metadata['source_path'] = str(path.resolve())
        container.metadata['source_format'] = 'json'
        container.add_file_path(path)
        print(f"Successfully extracted {len(df)} rows from JSON into a DataFrame.")
        return container