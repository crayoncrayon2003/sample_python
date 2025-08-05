# scripts/plugins/extractors/from_database.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class DatabaseExtractor(BaseExtractor):
    """
    Extracts data from a relational database by executing a SQL query.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.connection_string = self.params.get("connection_string")
        self.connection_details = self.params.get("connection_details")
        self.query = self.params.get("query")
        self.query_file = self.params.get("query_file")
        self.pandas_options = self.params.get("pandas_options", {})
        if not self.connection_string and not self.connection_details:
            raise ValueError("DatabaseExtractor requires 'connection_string' or 'connection_details'.")
        if not self.query and not self.query_file:
            raise ValueError("DatabaseExtractor requires either 'query' or 'query_file'.")

    def _get_connection_url(self) -> URL | str:
        if self.connection_string: return self.connection_string
        return URL.create(**self.connection_details)

    def _get_query(self) -> str:
        if self.query: return self.query
        try:
            with open(self.query_file, 'r', encoding='utf-8') as f: return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {self.query_file}")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        connection_url = self._get_connection_url()
        query = self._get_query()
        print(f"Connecting to database and executing query...")
        try:
            engine = create_engine(connection_url)
            df = pd.read_sql(sql=text(query), con=engine, **self.pandas_options)
            if not isinstance(df, pd.DataFrame):
                df = pd.concat(list(df), ignore_index=True)
        except Exception as e:
            print(f"Database extraction failed: {e}")
            raise

        print(f"Successfully extracted {len(df)} rows from the database.")
        container = DataContainer(data=df)
        sanitized_details = {k: v for k, v in self.connection_details.items() if 'password' not in k} if self.connection_details else {}
        container.metadata.update({'source_type': 'database', 'connection_details': sanitized_details})
        return container