# scripts/plugins/transformers/dataframe_joiner.py

from typing import Dict, Any, Optional
import pandas as pd
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DataFrameJoiner:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "dataframe_joiner"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        pandas_options = params.get("pandas_options", {})

        print(f"Joining data from {len(inputs)} inputs: {list(inputs.keys())}")
        dataframes_to_join = []
        for container in inputs.values():
            if container and container.data is not None:
                dataframes_to_join.append(container.data)

        if not dataframes_to_join:
            print("Warning: No DataFrames found in inputs to join.")
            return DataContainer()

        joined_df = pd.concat(dataframes_to_join, **pandas_options)
        print(f"Join complete. Resulting DataFrame has {len(joined_df)} rows.")
        return DataContainer(data=joined_df)