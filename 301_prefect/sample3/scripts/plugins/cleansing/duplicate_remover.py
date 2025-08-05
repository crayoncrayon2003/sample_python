# scripts/plugins/cleansing/duplicate_remover.py

from typing import Dict, Any, List, Union

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class DuplicateRemover(BaseCleanser):
    """
    Removes duplicate rows from a DataFrame within a DataContainer.

    This cleanser uses pandas' `drop_duplicates` method to identify and
    remove duplicate records based on a subset of columns or all columns.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the remover with parameters for duplicate detection.

        Expected params:
            - subset (list of str, optional): A list of column names to consider
              for identifying duplicates. If not provided, all columns are used.
            - keep (str or bool, optional): Determines which duplicates to keep.
              - 'first': (Default) Keep the first occurrence.
              - 'last': Keep the last occurrence.
              - False: Drop all duplicates.
              Defaults to 'first'.
        """
        super().__init__(params)
        self.subset: Union[List[str], None] = self.params.get("subset")
        self.keep: Union[str, bool] = self.params.get("keep", "first")

        if self.keep not in ['first', 'last', False]:
            raise ValueError("Parameter 'keep' must be one of 'first', 'last', or False.")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Applies the drop_duplicates logic to the DataFrame in the container.

        Args:
            data (DataContainer): The input container holding the DataFrame.

        Returns:
            DataContainer: A new container with the deduplicated DataFrame.
        """
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
            # Apply the drop_duplicates method from pandas
            deduplicated_df = df.drop_duplicates(
                subset=self.subset,
                keep=self.keep,
                inplace=False  # Always use inplace=False to create a new DataFrame
            )
        except KeyError as e:
            print(f"ERROR: One of the columns in the 'subset' parameter does not exist: {e}")
            raise

        final_row_count = len(deduplicated_df)
        rows_removed = initial_row_count - final_row_count
        print(f"Removed {rows_removed} duplicate rows. Final rows: {final_row_count}")

        # Create a new DataContainer with the modified DataFrame
        # to ensure immutability of the input container's data
        output_container = DataContainer(data=deduplicated_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duplicates_removed'] = {
            'removed_count': rows_removed,
            'subset': self.subset,
            'keep': self.keep
        }

        return output_container