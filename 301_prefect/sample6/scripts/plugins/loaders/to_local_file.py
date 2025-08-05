# scripts/plugins/loaders/to_local_file.py

from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import pluggy

from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

hookimpl = pluggy.HookimplMarker("etl_framework")

class LocalFileLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_local_file"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        output_path = Path(params.get("output_path"))
        file_format = SupportedFormats.from_string(params.get("format", "csv"))
        pandas_options = params.get("pandas_options", {})
        if not output_path: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_path'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: LocalFileLoader received no DataFrame.")
            return

        df = data.data
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Loading {len(df)} rows to local file: {output_path} (Format: {file_format.value})")

        try:
            if file_format == SupportedFormats.CSV:
                if 'index' not in pandas_options: pandas_options['index'] = False
                df.to_csv(output_path, **pandas_options)
            elif file_format == SupportedFormats.JSON:
                if 'orient' not in pandas_options: pandas_options['orient'] = 'records'
                df.to_json(output_path, **pandas_options)
            elif file_format == SupportedFormats.PARQUET:
                if 'index' not in pandas_options: pandas_options['index'] = False
                df.to_parquet(output_path, **pandas_options)
            else:
                raise ValueError(f"Unsupported format '{file_format.value}' for LocalFileLoader.")
        except Exception as e:
            print(f"ERROR saving file to {output_path}: {e}")
            raise
        print("File saved successfully.")
        # Loaders return None