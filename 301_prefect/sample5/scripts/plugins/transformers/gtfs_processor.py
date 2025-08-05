# scripts/plugins/transformers/gtfs_processor.py

from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class GtfsProcessor(BaseTransformer):
    """
    Processes a collection of GTFS files (stops.txt, routes.txt, etc.).
    """
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

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.pandas_options = self.params.get("pandas_options", {})

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("GtfsProcessor requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: GtfsProcessor received a DataContainer with no file paths. Skipping.")
            return data

        print("Processing GTFS files...")
        gtfs_dataframes: Dict[str, pd.DataFrame] = {}

        for file_path in data.file_paths:
            file_name = file_path.name
            if file_name in self.GTFS_FILES:
                table_name = file_name.replace('.txt', '')
                print(f"  Loading GTFS table: {file_name} as '{table_name}'")
                try:
                    df = pd.read_csv(file_path, **self.pandas_options)
                    gtfs_dataframes[table_name] = df
                except Exception as e:
                    print(f"  ERROR loading {file_name}: {e}. Skipping this file.")
                    continue
        
        if not gtfs_dataframes:
            print("Warning: No recognized GTFS files were found or loaded.")
            return DataContainer(data=data.data, metadata=data.metadata.copy())

        print(f"Loaded {len(gtfs_dataframes)} GTFS tables: {list(gtfs_dataframes.keys())}")
        primary_df = gtfs_dataframes.get('stops')

        output_container = DataContainer(data=primary_df)
        output_container.metadata = data.metadata.copy()
        output_container.metadata['gtfs_tables'] = gtfs_dataframes
        output_container.file_paths = data.file_paths.copy()

        return output_container