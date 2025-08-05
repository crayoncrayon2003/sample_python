# scripts/plugins/loaders/to_database.py

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DatabaseLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_database"

    def _get_connection_url(self, params: Dict[str, Any]) -> URL | str:
        if params.get("connection_string"): return params.get("connection_string")
        return URL.create(**params.get("connection_details"))

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        connection_string = params.get("connection_string")
        connection_details = params.get("connection_details")
        table_name = params.get("table_name")
        if_exists = params.get("if_exists", "fail")
        pandas_options = params.get("pandas_options", {})
        if not connection_string and not connection_details:
            raise ValueError("DatabaseLoader requires 'connection_string' or 'connection_details'.")
        if not table_name: raise ValueError("DatabaseLoader requires a 'table_name'.")
        if if_exists not in ['fail', 'replace', 'append']:
            raise ValueError("'if_exists' must be 'fail', 'replace', or 'append'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DatabaseLoader received no DataFrame.")
            return

        df = data.data
        connection_url = self._get_connection_url(params)
        print(f"Loading {len(df)} rows into database table '{table_name}'...")
        try:
            engine = create_engine(connection_url)
            if 'index' not in pandas_options: pandas_options['index'] = False
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, **pandas_options)
        except Exception as e:
            print(f"Database loading failed: {e}")
            raise
        print("Data loaded into database successfully.")