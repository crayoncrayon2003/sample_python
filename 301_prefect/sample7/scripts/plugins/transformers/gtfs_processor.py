# scripts/plugins/transformers/gtfs_processor.py

from typing import Dict, Any, Optional
import pandas as pd
import pluggy
from pathlib import Path

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class GtfsProcessor:
    """
    (File-based) Processes a directory of GTFS files into individual Parquet files.
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
        "feed_info.txt" ]

    @hookimpl
    def get_plugin_name(self) -> str:
        return "gtfs_processor"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_dir = Path(params.get("input_path"))
        output_dir = Path(params.get("output_path"))

        if not input_dir or not output_dir:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_dir.is_dir():
            raise NotADirectoryError(f"Input path '{input_dir}' is not a directory.")

        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Processing GTFS files in '{input_dir}'...")

        output_container = DataContainer()

        for file_name in self.GTFS_FILES:
            input_file = input_dir / file_name
            if input_file.exists():
                table_name = file_name.replace('.txt', '')
                output_file = output_dir / f"{table_name}.parquet"
                print(f"  Converting '{file_name}' to '{output_file.name}'...")
                try:
                    df = pd.read_csv(input_file)
                    df.to_parquet(output_file, index=False)
                    output_container.add_file_path(output_file)
                    output_container.metadata.setdefault('gtfs_tables', []).append(table_name)
                except Exception as e:
                    print(f"  ERROR processing {file_name}: {e}.")

        if not output_container.file_paths:
            print("Warning: No recognized GTFS files were processed.")

        return output_container