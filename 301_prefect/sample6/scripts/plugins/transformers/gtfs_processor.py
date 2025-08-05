# scripts/plugins/transformers/gtfs_processor.py

from typing import Dict, Any, Optional
import pandas as pd
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class GtfsProcessor:
    GTFS_FILES = [
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "fare_attributes.txt",
        "fare_rules.txt",
        "shapes.txt",
        "frequencies.txt",
        "transfers.txt",
        "pathways.txt",
        "levels.txt",
        "feed_info.txt"
    ]

    @hookimpl
    def get_plugin_name(self) -> str:
        return "gtfs_processor"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        pandas_options = params.get("pandas_options", {})
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: GtfsProcessor received no file paths.")
            return data

        print("Processing GTFS files...")
        gtfs_dataframes: Dict[str, pd.DataFrame] = {}
        for file_path in data.file_paths:
            if file_path.name in self.GTFS_FILES:
                table_name = file_path.name.replace('.txt', '')
                print(f"  Loading GTFS table: {file_path.name} as '{table_name}'")
                try:
                    df = pd.read_csv(file_path, **pandas_options)
                    gtfs_dataframes[table_name] = df
                except Exception as e:
                    print(f"  ERROR loading {file_path.name}: {e}.")

        if not gtfs_dataframes:
            print("Warning: No recognized GTFS files loaded.")
            return DataContainer(data=data.data, metadata=data.metadata.copy())

        print(f"Loaded {len(gtfs_dataframes)} GTFS tables.")
        output_container = DataContainer(data=gtfs_dataframes.get('stops'))
        output_container.metadata = data.metadata.copy()
        output_container.metadata['gtfs_tables'] = gtfs_dataframes
        output_container.file_paths = data.file_paths.copy()
        return output_container