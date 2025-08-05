# scripts/plugins/transformers/dataframe_joiner.py

from typing import Dict, Any, Optional
import pandas as pd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class DataFrameJoiner(BaseTransformer):
    """
    Joins (concatenates) multiple DataFrames from multiple input containers.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        # Options for pd.concat, e.g., {"axis": 0, "ignore_index": True}
        self.pandas_options = self.params.get("pandas_options", {})

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        print(f"Joining data from {len(inputs)} inputs: {list(inputs.keys())}")

        dataframes_to_join = []
        # inputs.values() から None を除外して DataFrame を収集
        for container in inputs.values():
            if container and container.data is not None:
                dataframes_to_join.append(container.data)

        if not dataframes_to_join:
            print("Warning: No DataFrames found in inputs to join.")
            return DataContainer()

        # Concatenate all found dataframes
        joined_df = pd.concat(dataframes_to_join, **self.pandas_options)
        print(f"Join complete. Resulting DataFrame has {len(joined_df)} rows.")

        # 新しいDataContainerを作成して返す
        return DataContainer(data=joined_df)