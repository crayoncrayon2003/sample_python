# backend/plugins/extractors/from_database.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DatabaseExtractor:
    """
    (File-based) Extracts data from a database and saves it to a local Parquet file.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_database"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "output_path": {
                    "type": "string",
                    "title": "Output Parquet Path",
                    "description": "The local path to save the extracted data as a Parquet file."
                },
                "query": {
                    "type": "string",
                    "title": "SQL Query",
                    "description": "The SQL query to execute to fetch the data. Use this or 'Query File'.",
                    "format": "textarea"
                },
                "query_file": {
                    "type": "string",
                    "title": "Query File",
                    "description": "Path to a file containing the SQL query. Use this or 'SQL Query'."
                },
                "connection_details": {
                    "type": "object",
                    "title": "Connection Details",
                    "description": "Database connection parameters (drivername, host, etc.).",
                    "properties": {
                        "drivername": {"type": "string", "default": "postgresql"},
                        "host": {"type": "string", "default": "localhost"},
                        "port": {"type": "integer", "default": 5432},
                        "database": {"type": "string"},
                        "username": {"type": "string"},
                        "password": {"type": "string", "format": "password"}
                    }
                }
            },
            "required": ["output_path"]
        }

    def _get_connection_url(self, params: Dict[str, Any]) -> URL | str:
        if params.get("connection_string"):
            return params.get("connection_string")
        if not params.get("connection_details"):
            raise ValueError("Database connection requires 'connection_details' or 'connection_string'.")
        return URL.create(**params.get("connection_details"))

    def _get_query(self, params: Dict[str, Any]) -> str:
        if params.get("query"):
            return params.get("query")
        query_file_path = params.get("query_file")
        if not query_file_path:
            raise ValueError("Database query requires 'query' or 'query_file'.")
        try:
            with open(query_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found at: {query_file_path}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        output_path = Path(params.get("output_path"))
        pandas_read_options = params.get("pandas_read_options", {})

        if not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_path'.")
        if inputs:
            print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        connection_url = self._get_connection_url(params)
        sql_query = self._get_query(params)

        print(f"Connecting to database and executing query...")
        try:
            engine = create_engine(connection_url)
            df = pd.read_sql(sql=text(sql_query), con=engine, **pandas_read_options)
            if not isinstance(df, pd.DataFrame):
                df = pd.concat(list(df), ignore_index=True)
        except Exception as e:
            print(f"Database extraction failed: {e}")
            raise

        print(f"Successfully extracted {len(df)} rows. Saving to Parquet file '{output_path}'...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)

        container = DataContainer()
        container.add_file_path(output_path)
        connection_details = params.get("connection_details", {})
        sanitized_details = {k: v for k, v in connection_details.items() if 'password' not in k}
        container.metadata.update({'source_type': 'database', 'connection_details': sanitized_details})
        return container