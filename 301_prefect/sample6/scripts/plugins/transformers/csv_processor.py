# scripts/plugins/transformers/csv_processor.py

from typing import Dict, Any, Optional
import pandas as pd
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class CsvProcessor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "csv_processor"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        rename_map = params.get("rename_columns")
        use_columns = params.get("use_columns")
        skip_rows = params.get("skip_rows")
        skip_footer = params.get("skip_footer")
        index_col = params.get("set_index")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: CsvProcessor received no DataFrame.")
            return data

        df = data.data.copy()
        print(f"Processing CSV data. Initial shape: {df.shape}")
        if skip_rows is not None or skip_footer is not None:
            start, end = skip_rows or 0, - (skip_footer or 0)
            if end == 0: end = None
            df = df.iloc[start:end].reset_index(drop=True)
        if rename_map: df.rename(columns=rename_map, inplace=True)
        if use_columns: df = df[use_columns]
        if index_col: df.set_index(index_col, inplace=True)
        print(f"CSV processing complete. Final shape: {df.shape}")

        output_container = DataContainer(data=df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['csv_processed'] = True
        return output_container