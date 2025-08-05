# scripts/plugins/extractors/from_database.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DatabaseExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_database"

    def _get_connection_url(self, params: Dict[str, Any]) -> URL | str:
        if params.get("connection_string"): return params.get("connection_string")
        return URL.create(**params.get("connection_details"))

    def _get_query(self, params: Dict[str, Any]) -> str:
        if params.get("query"): return params.get("query")
        try:
            with open(params.get("query_file"), 'r', encoding='utf-8') as f: return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {params.get('query_file')}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        connection_string = params.get("connection_string")
        connection_details = params.get("connection_details")
        query = params.get("query")
        query_file = params.get("query_file")
        pandas_options = params.get("pandas_options", {})
        if not connection_string and not connection_details:
            raise ValueError("DatabaseExtractor requires 'connection_string' or 'connection_details'.")
        if not query and not query_file:
            raise ValueError("DatabaseExtractor requires either 'query' or 'query_file'.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        connection_url = self._get_connection_url(params)
        sql_query = self._get_query(params)
        print(f"Connecting to database and executing query...")
        try:
            engine = create_engine(connection_url)
            df = pd.read_sql(sql=text(sql_query), con=engine, **pandas_options)
            if not isinstance(df, pd.DataFrame):
                df = pd.concat(list(df), ignore_index=True)
        except Exception as e:
            print(f"Database extraction failed: {e}")
            raise

        print(f"Successfully extracted {len(df)} rows from the database.")
        container = DataContainer(data=df)
        sanitized_details = {k: v for k, v in connection_details.items() if 'password' not in k} if connection_details else {}
        container.metadata.update({'source_type': 'database', 'connection_details': sanitized_details})
        return container