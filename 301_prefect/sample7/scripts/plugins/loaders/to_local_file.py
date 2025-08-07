# scripts/plugins/loaders/to_local_file.py

from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import shutil
import pandas as pd

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class LocalFileLoader:
    """
    (File-based) Loads data by moving/copying an intermediate file to a final destination.
    It can also reformat the file during the process.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_local_file"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        final_format = params.get("format")

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file to load not found at: {input_path}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Loading file from '{input_path}' to final destination '{output_path}'.")

        try:
            if final_format:
                print(f"Re-formatting to '{final_format}'...")
                df = pd.read_parquet(input_path)

                if final_format == 'csv':
                    df.to_csv(output_path, index=False)
                elif final_format == 'json':
                    df.to_json(output_path, orient='records', lines=True)
                elif final_format == 'parquet':
                     df.to_parquet(output_path, index=False)
                else:
                    raise ValueError(f"Unsupported final format '{final_format}'.")
            else:
                shutil.copy(input_path, output_path)
        except Exception as e:
            print(f"ERROR during final load to local file: {e}")
            raise

        print("Load to local file system complete.")
        return None