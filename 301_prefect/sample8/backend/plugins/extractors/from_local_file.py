# backend/plugins/extractors/from_local_file.py

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class LocalFileExtractor:
    """
    (File-based) Reads a source file (like CSV) and saves it as Parquet
    to serve as the starting point for a file-based pipeline.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_local_file"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "title": "Source File Path",
                    "description": "The path to the source file (e.g., CSV) to be read."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Staging Path",
                    "description": "Path where the initial Parquet file will be staged."
                },
                "read_options": {
                    "type": "object",
                    "title": "Pandas Read Options",
                    "description": "(Optional) Advanced options for the pandas read_csv function.",
                    "default": {}
                }
            },
            "required": ["source_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        source_path = Path(params.get("source_path"))
        output_path = Path(params.get("output_path"))
        read_options = params.get("read_options", {})

        if not source_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'source_path' and 'output_path' parameters.")
        if inputs:
            print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        print(f"Reading source file '{source_path}'...")
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found at: {source_path}")

        try:
            # Assume the source is CSV. This could be made more flexible.
            df = pd.read_csv(source_path, **read_options)

            print(f"Read {len(df)} rows. Converting and saving to Parquet at '{output_path}'.")

            output_path.parent.mkdir(parents=True, exist_ok=True)
            # データをParquet形式で保存
            df.to_parquet(output_path, index=False)

        except Exception as e:
            print(f"Failed to process source file {source_path}: {e}")
            raise

        container = DataContainer()
        container.add_file_path(output_path)
        container.metadata['source_path'] = str(source_path)

        print(f"Successfully staged source data to {output_path}.")
        return container