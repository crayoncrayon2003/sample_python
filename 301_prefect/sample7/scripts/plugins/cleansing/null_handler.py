# scripts/plugins/cleansing/null_handler.py

from typing import Dict, Any, Optional
import pluggy
import pandas as pd
from pathlib import Path

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NullHandler:
    """
    (File-based) Handles missing values in a tabular file.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "null_handler"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        strategy = params.get("strategy")
        read_options = params.get("read_options", {})

        if not input_path or not output_path or not strategy:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path', 'output_path', and 'strategy'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading file '{input_path}' to handle nulls with strategy '{strategy}'...")
        df = pd.read_parquet(input_path, **read_options)

        initial_null_counts = df.isnull().sum().sum()
        print(f"Initial total nulls: {initial_null_counts}")

        processed_df = df.copy()
        if strategy == 'drop_row':
            processed_df.dropna(axis=0, subset=params.get("subset"), inplace=True)
        elif strategy == 'fill':
            processed_df.fillna(value=params.get("value"), method=params.get("method"), inplace=True)
        else:
            raise ValueError(f"Unsupported strategy: '{strategy}'. Supported are 'drop_row', 'fill'.")

        final_null_counts = processed_df.isnull().sum().sum()
        print(f"Null handling complete. Final total nulls: {final_null_counts}. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        processed_df.to_parquet(output_path, index=False)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['null_handler_applied'] = {'strategy': strategy}
        return output_container