# scripts/plugins/cleansing/duplicate_remover.py

from typing import Dict, Any, Union, List, Optional
import pluggy
import pandas as pd
from pathlib import Path

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuplicateRemover:
    """
    (File-based) Removes duplicate rows from a tabular file (e.g., Parquet, CSV).
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "duplicate_remover"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        subset: Union[List[str], None] = params.get("subset")
        keep: Union[str, bool] = params.get("keep", "first")
        read_options = params.get("read_options", {})

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path' parameters.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        # Assuming Parquet for intermediate files.
        print(f"Reading file '{input_path}' to remove duplicates...")
        df = pd.read_parquet(input_path, **read_options)

        initial_row_count = len(df)
        print(f"Initial rows: {initial_row_count}")

        deduplicated_df = df.drop_duplicates(subset=subset, keep=keep, inplace=False)

        rows_removed = initial_row_count - len(deduplicated_df)
        print(f"Removed {rows_removed} duplicate rows. Saving {len(deduplicated_df)} rows to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        deduplicated_df.to_parquet(output_path, index=False)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['duplicates_removed'] = {'removed_count': rows_removed}
        return output_container