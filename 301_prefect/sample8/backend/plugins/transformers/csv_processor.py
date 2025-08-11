# backend/plugins/transformers/csv_processor.py

from typing import Dict, Any, Optional
import pandas as pd
import pluggy
from pathlib import Path

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class CsvProcessor:
    """
    (File-based) Performs common transformations on a CSV file and saves the result.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "csv_processor"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input CSV Path",
                    "description": "The source CSV file to be processed."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Parquet Path",
                    "description": "Path to save the processed data as a Parquet file."
                },
                "rename_columns": {
                    "type": "object",
                    "title": "Rename Columns (Optional)",
                    "description": "A mapping of old column names to new column names."
                },
                "use_columns": {
                    "type": "array",
                    "title": "Use Columns (Optional)",
                    "description": "A list of columns to keep; others will be dropped.",
                    "items": {"type": "string"}
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        rename_map = params.get("rename_columns")
        use_columns = params.get("use_columns")
        skip_rows = params.get("skip_rows")
        skip_footer = params.get("skip_footer")
        index_col = params.get("set_index")
        read_options = params.get("read_options", {})

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading CSV '{input_path}' for processing...")
        df = pd.read_csv(input_path, **read_options)
        print(f"Initial shape: {df.shape}")

        if skip_rows is not None or skip_footer is not None:
            start, end = skip_rows or 0, - (skip_footer or 0)
            if end == 0: end = None
            df = df.iloc[start:end].reset_index(drop=True)
        if rename_map: df.rename(columns=rename_map, inplace=True)
        if use_columns: df = df[use_columns]
        if index_col: df.set_index(index_col, inplace=True)

        print(f"Processing complete. Final shape: {df.shape}. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Output is standardized to Parquet
        df.to_parquet(output_path, index=False)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container