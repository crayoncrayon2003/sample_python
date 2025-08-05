# scripts/plugins/transformers/csv_processor.py

from typing import Dict, Any, List, Union
import pandas as pd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class CsvProcessor(BaseTransformer):
    """
    Performs common transformation tasks on data that originated from CSV files.

    This transformer provides a convenient way to apply typical CSV cleaning
    and shaping operations, such as renaming columns, selecting a subset
    of columns, skipping rows, and setting a new index, all powered by pandas.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the processor with CSV transformation parameters.

        Expected params:
            - rename_columns (dict, optional): A dictionary to rename columns,
              e.g., {'old_name': 'new_name'}.
            - use_columns (list of str, optional): A list of column names to keep.
              All other columns will be dropped.
            - skip_rows (int, optional): Number of rows to skip from the top
              of the DataFrame.
            - skip_footer (int, optional): Number of rows to skip from the bottom.
            - set_index (str, optional): The name of the column to set as the
              DataFrame's index.
        """
        super().__init__(params)
        self.rename_map = self.params.get("rename_columns")
        self.use_columns = self.params.get("use_columns")
        self.skip_rows = self.params.get("skip_rows")
        self.skip_footer = self.params.get("skip_footer")
        self.index_col = self.params.get("set_index")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Applies a series of CSV-specific transformations to the DataFrame.

        Args:
            data (DataContainer): The input container with the DataFrame to process.

        Returns:
            DataContainer: A new container with the transformed DataFrame.
        """
        if data.data is None:
            print("Warning: CsvProcessor received a DataContainer with no DataFrame. Skipping.")
            return data

        # Start with a copy to avoid modifying the original DataFrame in the container
        df = data.data.copy()
        print(f"Processing CSV data. Initial shape: {df.shape}")

        # 1. Skip rows from top and bottom
        if self.skip_rows is not None or self.skip_footer is not None:
            # Slicing the DataFrame to skip rows
            start = self.skip_rows or 0
            end = - (self.skip_footer or 0)
            if end == 0: end = None # Handle case where skip_footer is 0 or None
            
            df = df.iloc[start:end].reset_index(drop=True)
            print(f"Skipped rows. New shape: {df.shape}")

        # 2. Rename columns
        if self.rename_map:
            try:
                df.rename(columns=self.rename_map, inplace=True)
                print(f"Renamed columns. New names: {list(df.columns)}")
            except TypeError as e:
                print(f"ERROR renaming columns. Ensure 'rename_columns' is a dictionary. Error: {e}")
                raise

        # 3. Select a subset of columns
        if self.use_columns:
            try:
                df = df[self.use_columns]
                print(f"Selected a subset of columns. Final columns: {self.use_columns}")
            except KeyError as e:
                print(f"ERROR: A column in 'use_columns' does not exist: {e}")
                raise
        
        # 4. Set the index
        if self.index_col:
            try:
                df.set_index(self.index_col, inplace=True)
                print(f"Set '{self.index_col}' as the new index.")
            except KeyError as e:
                print(f"ERROR: The specified index column '{self.index_col}' does not exist: {e}")
                raise

        print(f"CSV processing complete. Final shape: {df.shape}")

        output_container = DataContainer(data=df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['csv_processed'] = True

        return output_container