# scripts/plugins/loaders/to_database.py

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional
import pluggy
import pandas as pd
from pathlib import Path

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DatabaseLoader:
    """
    (File-based) Loads data from a file into a database table.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_database"

    def _get_connection_url(self, params: Dict[str, Any]) -> URL | str:
        if params.get("connection_string"): return params.get("connection_string")
        if not params.get("connection_details"):
            raise ValueError("DB connection requires 'connection_details' or 'connection_string'.")
        return URL.create(**params.get("connection_details"))

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        table_name = params.get("table_name")
        if_exists = params.get("if_exists", "fail")
        pandas_options = params.get("pandas_options", {})

        if not input_path or not table_name:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'table_name'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading file '{input_path}' to load into database table '{table_name}'...")
        df = pd.read_parquet(input_path)

        connection_url = self._get_connection_url(params)
        print(f"Loading {len(df)} rows into database...")
        try:
            engine = create_engine(connection_url)
            if 'index' not in pandas_options: pandas_options['index'] = False
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, **pandas_options)
        except Exception as e:
            print(f"Database loading failed: {e}")
            raise

        print("Data loaded into database successfully.")
        return None