# scripts/plugins/cleansing/null_handler.py

from typing import Dict, Any, List, Union

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class NullHandler(BaseCleanser):
    """
    Handles missing values (NaN, None) in a DataFrame.

    This cleanser provides strategies to deal with nulls, such as dropping
    rows/columns or filling them with a specific value, using pandas'
    `dropna` and `fillna` methods.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the handler with a strategy for null values.

        Expected params:
            - strategy (str): The method to use for handling nulls.
              Supported strategies: 'drop_row', 'drop_col', 'fill'.
            - subset (list of str, optional): For 'drop_row' or 'drop_col',
              a list of columns/labels to consider when looking for nulls.
            - value (any, optional): For the 'fill' strategy, the value to
              use for filling nulls.
            - method (str, optional): For the 'fill' strategy, the interpolation
              method to use (e.g., 'ffill' for forward-fill, 'bfill' for
              backward-fill).
        """
        super().__init__(params)
        self.strategy = self.params.get("strategy")
        self.subset = self.params.get("subset")
        self.value = self.params.get("value")
        self.method = self.params.get("method")

        if not self.strategy or self.strategy not in ['drop_row', 'drop_col', 'fill']:
            raise ValueError("NullHandler requires a 'strategy' parameter, which must be 'drop_row', 'drop_col', or 'fill'.")
        if self.strategy == 'fill' and self.value is None and self.method is None:
            raise ValueError("The 'fill' strategy requires either a 'value' or a 'method' parameter.")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Applies the chosen null handling strategy to the DataFrame.

        Args:
            data (DataContainer): The input container with the DataFrame to process.

        Returns:
            DataContainer: A new container with the processed DataFrame.
        """
        if data.data is None:
            print("Warning: NullHandler received a DataContainer with no DataFrame. Skipping.")
            return data

        df = data.data
        initial_null_counts = df.isnull().sum().sum()
        print(f"Handling null values. Strategy: '{self.strategy}'. Initial total nulls: {initial_null_counts}")

        processed_df = df.copy() # Work on a copy to ensure immutability

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