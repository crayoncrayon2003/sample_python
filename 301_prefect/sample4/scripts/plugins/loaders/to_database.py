# scripts/plugins/loaders/to_database.py

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from typing import Dict, Any

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class DatabaseLoader(BaseLoader):
    """
    Loads data from a DataFrame into a database table.

    This loader uses SQLAlchemy and pandas' `to_sql` method to efficiently
    write the contents of a DataFrame to a specified table in a database.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with database connection and table details.

        Expected params:
            - connection_string (str): A full SQLAlchemy connection string.
              OR
            - connection_details (dict): A dictionary with connection details.
            
            - table_name (str): The name of the target table in the database.
            - if_exists (str, optional): How to behave if the table already exists.
              - 'fail': (Default) Raise a ValueError.
              - 'replace': Drop the table before inserting new values.
              - 'append': Insert new values to the existing table.
            - pandas_options (dict, optional): Other options to pass to pandas'
              `to_sql` function (e.g., `chunksize`, `index`).
        """
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
        """Builds the connection URL from the provided parameters."""
        if self.connection_string:
            return self.connection_string
        return URL.create(**self.connection_details)

    def execute(self, data: DataContainer) -> None:
        """
        Connects to the database and writes the DataFrame to the specified table.

        Args:
            data (DataContainer): The container with the DataFrame to load.
        """
        if data.data is None:
            print("Warning: DatabaseLoader received a DataContainer with no DataFrame. Skipping.")
            return

        df = data.data
        connection_url = self._get_connection_url()

        print(f"Loading {len(df)} rows into database table '{self.table_name}'...")
        
        try:
            engine = create_engine(connection_url)

            # Default to not writing the pandas index to the database
            if 'index' not in self.pandas_options:
                self.pandas_options['index'] = False

            # Use pandas' to_sql method for loading
            df.to_sql(
                name=self.table_name,
                con=engine,
                if_exists=self.if_exists,
                **self.pandas_options
            )

        except Exception as e:
            print(f"Database loading failed: {e}")
            raise

        print("Data loaded into database successfully.")