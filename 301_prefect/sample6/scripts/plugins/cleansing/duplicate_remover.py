# scripts/plugins/cleansing/duplicate_remover.py

from typing import Dict, Any, List, Union, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuplicateRemover:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "duplicate_remover"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        subset: Union[List[str], None] = params.get("subset")
        keep: Union[str, bool] = params.get("keep", "first")
        if keep not in ['first', 'last', False]:
            raise ValueError("Parameter 'keep' must be one of 'first', 'last', or False.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DuplicateRemover received no DataFrame.")
            return data

        df = data.data
        initial_row_count = len(df)
        print(f"Removing duplicates. Initial rows: {initial_row_count}")
        if subset: print(f"Using subset of columns: {subset}")
        else: print("Using all columns.")

        try:
            deduplicated_df = df.drop_duplicates(subset=subset, keep=keep, inplace=False)
        except KeyError as e:
            raise KeyError(f"ERROR: A column in the 'subset' does not exist: {e}")

        rows_removed = initial_row_count - len(deduplicated_df)
        print(f"Removed {rows_removed} duplicate rows. Final rows: {len(deduplicated_df)}")

        output_container = DataContainer(data=deduplicated_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duplicates_removed'] = {'removed_count': rows_removed, 'subset': subset, 'keep': keep}
        return output_container