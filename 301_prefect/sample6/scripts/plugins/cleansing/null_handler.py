# scripts/plugins/cleansing/null_handler.py

from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NullHandler:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "null_handler"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        strategy = params.get("strategy")
        subset = params.get("subset")
        value = params.get("value")
        method = params.get("method")
        if not strategy or strategy not in ['drop_row', 'drop_col', 'fill']:
            raise ValueError("NullHandler requires a 'strategy': 'drop_row', 'drop_col', or 'fill'.")
        if strategy == 'fill' and value is None and method is None:
            raise ValueError("The 'fill' strategy requires either a 'value' or a 'method' parameter.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: NullHandler received no DataFrame.")
            return data

        df = data.data
        initial_null_counts = df.isnull().sum().sum()
        print(f"Handling nulls with strategy '{strategy}'. Initial total nulls: {initial_null_counts}")

        processed_df = df.copy()
        try:
            if strategy == 'drop_row':
                processed_df.dropna(axis=0, subset=subset, inplace=True)
            elif strategy == 'drop_col':
                processed_df.dropna(axis=1, subset=subset, inplace=True)
            elif strategy == 'fill':
                if value is not None: processed_df.fillna(value=value, inplace=True)
                elif method: processed_df.fillna(method=method, inplace=True)
        except KeyError as e:
            raise KeyError(f"ERROR: A column in 'subset' does not exist: {e}")

        final_null_counts = processed_df.isnull().sum().sum()
        print(f"Null handling complete. Final total nulls: {final_null_counts}")

        output_container = DataContainer(data=processed_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['null_handler_applied'] = {'strategy': strategy, 'initial_nulls': int(initial_null_counts), 'final_nulls': int(final_null_counts)}
        return output_container