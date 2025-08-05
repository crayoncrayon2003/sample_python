# scripts/plugins/cleansing/duplicate_remover.py

from typing import Dict, Any, List, Union, Optional

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class DuplicateRemover(BaseCleanser):
    """
    Removes duplicate rows from a DataFrame within a DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.subset: Union[List[str], None] = self.params.get("subset")
        self.keep: Union[str, bool] = self.params.get("keep", "first")
        if self.keep not in ['first', 'last', False]:
            raise ValueError("Parameter 'keep' must be one of 'first', 'last', or False.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("DuplicateRemover requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DuplicateRemover received a DataContainer with no DataFrame. Skipping.")
            return data

        df = data.data
        initial_row_count = len(df)
        print(f"Removing duplicates. Initial rows: {initial_row_count}")
        if self.subset:
            print(f"Using subset of columns for deduplication: {self.subset}")
        else:
            print("Using all columns for deduplication.")

        try:
            deduplicated_df = df.drop_duplicates(
                subset=self.subset,
                keep=self.keep,
                inplace=False
            )
        except KeyError as e:
            print(f"ERROR: One of the columns in the 'subset' parameter does not exist: {e}")
            raise

        final_row_count = len(deduplicated_df)
        rows_removed = initial_row_count - final_row_count
        print(f"Removed {rows_removed} duplicate rows. Final rows: {final_row_count}")

        output_container = DataContainer(data=deduplicated_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duplicates_removed'] = {
            'removed_count': rows_removed,
            'subset': self.subset,
            'keep': self.keep
        }

        return output_container