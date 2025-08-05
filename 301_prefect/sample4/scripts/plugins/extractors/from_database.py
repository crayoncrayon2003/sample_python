# scripts/plugins/extractors/from_database.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class DatabaseExtractor(BaseExtractor):
    """
    Extracts data from a relational database by executing a SQL query.

    This extractor uses SQLAlchemy to connect to a wide variety of databases
    (PostgreSQL, MySQL, SQLite, etc.) and runs a user-provided SQL query
    to fetch data directly into a pandas DataFrame.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with database connection details and a query.

        Expected params:
            - connection_string (str): A full SQLAlchemy connection string.
              e.g., "postgresql://user:password@host:port/dbname"
              OR
            - connection_details (dict): A dictionary with keys like 'drivername',
              'username', 'password', 'host', 'port', 'database'.
            
            - query (str): The SQL query to execute to fetch the data.
              OR
            - query_file (str): Path to a file containing the SQL query.
            
            - pandas_options (dict, optional): Options to pass to pandas'
              `read_sql` function, e.g., `chunksize`.
        """
        super().__init__(params)
        
        self.connection_string = self.params.get("connection_string")
        self.connection_details = self.params.get("connection_details")
        self.query = self.params.get("query")
        self.query_file = self.params.get("query_file")
        self.pandas_options = self.params.get("pandas_options", {})

        if not self.connection_string and not self.connection_details:
            raise ValueError("DatabaseExtractor requires either 'connection_string' or 'connection_details'.")
        if not self.query and not self.query_file:
            raise ValueError("DatabaseExtractor requires either 'query' or 'query_file'.")

    def _get_connection_url(self) -> URL | str:
        """Builds the connection URL from the provided parameters."""
        if self.connection_string:
            return self.connection_string
        return URL.create(**self.connection_details)

    def _get_query(self) -> str:
        """Loads the SQL query from a file or uses the inline query."""
        if self.query:
            return self.query
        
        try:
            with open(self.query_file, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {self.query_file}")

    def execute(self) -> DataContainer:
        """
        Connects to the database, executes the query, and returns the result.

        Returns:
            DataContainer: A container with the query result in its DataFrame.
        
        Raises:
            Exception: If there is an error connecting to the database or
                       executing the query.
        """
        connection_url = self._get_connection_url()
        query = self._get_query()
        
        print(f"Connecting to database and executing query...")
        
        try:
            # Create a SQLAlchemy engine
            engine = create_engine(connection_url)

            # Use pandas' read_sql function, which is highly optimized for this task
            df = pd.read_sql(sql=text(query), con=engine, **self.pandas_options)
            
            # For large datasets, read_sql can return an iterator of DataFrames
            # if `chunksize` is specified. Here, we concatenate them for simplicity.
            # A more advanced implementation might handle chunks iteratively.
            if not isinstance(df, pd.DataFrame):
                df = pd.concat(list(df), ignore_index=True)

        except Exception as e:
            print(f"Database extraction failed: {e}")
            raise

        print(f"Successfully extracted {len(df)} rows from the database.")
        
        container = DataContainer(data=df)
        # Avoid logging passwords in metadata
        sanitized_details = {k: v for k, v in self.connection_details.items() if 'password' not in k} if self.connection_details else {}
        container.metadata['source_type'] = 'database'
        container.metadata['connection_details'] = sanitized_details
        
        return container