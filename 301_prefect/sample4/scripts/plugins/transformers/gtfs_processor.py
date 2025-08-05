# scripts/plugins/transformers/gtfs_processor.py

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class GtfsProcessor(BaseTransformer):
    """
    Processes a collection of GTFS files (stops.txt, routes.txt, etc.).

    This transformer identifies standard GTFS files from the input file paths,
    loads each one into a separate pandas DataFrame, and stores them in a
    special dictionary within the DataContainer's metadata. This allows
    subsequent steps to easily access individual GTFS tables.
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
        "feed_info.txt",
    ]

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the GTFS processor.

        Expected params:
            - pandas_options (dict, optional): A dictionary of options to pass
              to the `pd.read_csv` function for all GTFS files.
        """
        super().__init__(params)
        self.pandas_options = self.params.get("pandas_options", {})

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Loads recognized GTFS files into separate DataFrames.

        Args:
            data (DataContainer): The input container, expected to have paths
                                  to the extracted GTFS .txt files.

        Returns:
            DataContainer: A new container where the primary data might be one of
                           the main GTFS tables (e.g., stops), and the metadata
                           contains a dictionary of all loaded GTFS DataFrames.
        """
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
            # Return a copy of the input container
            return DataContainer(
                data=data.data,
                metadata=data.metadata.copy()
            )

        print(f"Loaded {len(gtfs_dataframes)} GTFS tables: {list(gtfs_dataframes.keys())}")

        # For the primary data of the container, we can choose a main table,
        # for example, 'stops'. This is an arbitrary choice but can be useful.
        primary_df = gtfs_dataframes.get('stops')

        # Create a new DataContainer
        output_container = DataContainer(data=primary_df)
        output_container.metadata = data.metadata.copy()
        
        # Store all loaded GTFS DataFrames in the metadata for later access
        output_container.metadata['gtfs_tables'] = gtfs_dataframes
        
        # Keep the file paths for reference
        output_container.file_paths = data.file_paths.copy()

        return output_container