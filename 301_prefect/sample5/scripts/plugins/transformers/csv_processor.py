# scripts/plugins/transformers/csv_processor.py

from typing import Dict, Any, List, Union, Optional
import pandas as pd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class CsvProcessor(BaseTransformer):
    """
    Performs common transformation tasks on data that originated from CSV files.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.rename_map = self.params.get("rename_columns")
        self.use_columns = self.params.get("use_columns")
        self.skip_rows = self.params.get("skip_rows")
        self.skip_footer = self.params.get("skip_footer")
        self.index_col = self.params.get("set_index")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("CsvProcessor requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: CsvProcessor received a DataContainer with no DataFrame. Skipping.")
            return data

        df = data.data.copy()
        print(f"Processing CSV data. Initial shape: {df.shape}")

        if self.skip_rows is not None or self.skip_footer is not None:
            start = self.skip_rows or 0
            end = - (self.skip_footer or 0)
            if end == 0: end = None
            df = df.iloc[start:end].reset_index(drop=True)
            print(f"Skipped rows. New shape: {df.shape}")

        if self.rename_map:
            try:
                df.rename(columns=self.rename_map, inplace=True)
                print(f"Renamed columns. New names: {list(df.columns)}")
            except TypeError as e:
                print(f"ERROR renaming columns. Ensure 'rename_columns' is a dictionary. Error: {e}")
                raise

        if self.use_columns:
            try:
                df = df[self.use_columns]
                print(f"Selected a subset of columns. Final columns: {self.use_columns}")
            except KeyError as e:
                print(f"ERROR: A column in 'use_columns' does not exist: {e}")
                raise

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