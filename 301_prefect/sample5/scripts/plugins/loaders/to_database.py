# scripts/plugins/loaders/to_database.py

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any, Optional

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class DatabaseLoader(BaseLoader):
    """
    Loads data from a DataFrame into a database table.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.connection_string = self.params.get("connection_string")
        self.connection_details = self.params.get("connection_details")
        self.table_name = self.params.get("table_name")
        self.if_exists = self.params.get("if_exists", "fail")
        self.pandas_options = self.params.get("pandas_options", {})

        if not self.connection_string and not self.connection_details:
            raise ValueError("DatabaseLoader requires either 'connection_string' or 'connection_details'.")
        if not self.table_name:
            raise ValueError("DatabaseLoader requires a 'table_name' parameter.")
        if self.if_exists not in ['fail', 'replace', 'append']:
            raise ValueError("'if_exists' must be one of 'fail', 'replace', or 'append'.")

    def _get_connection_url(self) -> URL | str:
        if self.connection_string: return self.connection_string
        return URL.create(**self.connection_details)

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> None:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("DatabaseLoader requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DatabaseLoader received a DataContainer with no DataFrame. Skipping.")
            return

        df = data.data
        connection_url = self._get_connection_url()
        print(f"Loading {len(df)} rows into database table '{self.table_name}'...")
        
        try:
            engine = create_engine(connection_url)
            if 'index' not in self.pandas_options: self.pandas_options['index'] = False
            df.to_sql(
                name=self.table_name, con=engine,
                if_exists=self.if_exists, **self.pandas_options
            )
        except Exception as e:
            print(f"Database loading failed: {e}")
            raise

        print("Data loaded into database successfully.")