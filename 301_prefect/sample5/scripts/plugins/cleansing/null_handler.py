# scripts/plugins/cleansing/null_handler.py

from typing import Dict, Any, List, Union, Optional

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class NullHandler(BaseCleanser):
    """
    Handles missing values (NaN, None) in a DataFrame.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.strategy = self.params.get("strategy")
        self.subset = self.params.get("subset")
        self.value = self.params.get("value")
        self.method = self.params.get("method")
        if not self.strategy or self.strategy not in ['drop_row', 'drop_col', 'fill']:
            raise ValueError("NullHandler requires a 'strategy' parameter, which must be 'drop_row', 'drop_col', or 'fill'.")
        if self.strategy == 'fill' and self.value is None and self.method is None:
            raise ValueError("The 'fill' strategy requires either a 'value' or a 'method' parameter.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("NullHandler requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: NullHandler received a DataContainer with no DataFrame. Skipping.")
            return data

        df = data.data
        initial_null_counts = df.isnull().sum().sum()
        print(f"Handling null values. Strategy: '{self.strategy}'. Initial total nulls: {initial_null_counts}")

        processed_df = df.copy()

        try:
            if self.strategy == 'drop_row':
                processed_df.dropna(axis=0, subset=self.subset, inplace=True)
                print(f"Dropped rows with null values. Columns considered: {self.subset or 'all'}")
            elif self.strategy == 'drop_col':
                processed_df.dropna(axis=1, subset=self.subset, inplace=True)
                print(f"Dropped columns with null values. Rows considered: {self.subset or 'all'}")
            elif self.strategy == 'fill':
                if self.value is not None:
                    processed_df.fillna(value=self.value, inplace=True)
                    print(f"Filled null values with: {self.value}")
                elif self.method:
                    processed_df.fillna(method=self.method, inplace=True)
                    print(f"Filled null values using method: {self.method}")
        except KeyError as e:
            print(f"ERROR: A column specified in 'subset' does not exist: {e}")
            raise

        final_null_counts = processed_df.isnull().sum().sum()
        print(f"Null handling complete. Final total nulls: {final_null_counts}")

        output_container = DataContainer(data=processed_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['null_handler_applied'] = {
            'strategy': self.strategy,
            'initial_nulls': int(initial_null_counts),
            'final_nulls': int(final_null_counts)
        }

        return output_container