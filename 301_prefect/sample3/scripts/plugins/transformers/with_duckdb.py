# scripts/plugins/transformers/with_duckdb.py

import duckdb
from typing import Dict, Any

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class DuckDBTransformer(BaseTransformer):
    """
    Transforms data using SQL queries powered by DuckDB.

    This transformer loads a DataFrame from the input DataContainer into an
    in-memory DuckDB database as a table. It then executes a user-provided
    SQL query against this table to perform transformations. The result of
    the query is then returned as a new DataFrame in the output DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the transformer with a SQL query.

        Expected params:
            - query (str): The SQL query to execute against the DataFrame.
              The DataFrame will be available as a table named 'source_data'.
              OR
            - query_file (str): The path to a file containing the SQL query.
            - table_name (str, optional): The name to register the DataFrame as
              in DuckDB. Defaults to 'source_data'.
        """
        super().__init__(params)
        self.query = self.params.get("query")
        self.query_file = self.params.get("query_file")
        self.table_name = self.params.get("table_name", "source_data")

        if not self.query and not self.query_file:
            raise ValueError("DuckDBTransformer requires either a 'query' or 'query_file' parameter.")

    def _get_query(self) -> str:
        """Loads the SQL query from a file or uses the inline query."""
        if self.query:
            return self.query
        
        try:
            with open(self.query_file, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {self.query_file}")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Loads the DataFrame into DuckDB, executes the SQL query, and returns the result.

        Args:
            data (DataContainer): The input container holding the DataFrame to transform.

        Returns:
            DataContainer: A new container with the transformed DataFrame.
        """
        if data.data is None:
            print("Warning: DuckDBTransformer received a DataContainer with no DataFrame. Skipping.")
            return data

        source_df = data.data
        query = self._get_query()
        
        print(f"Transforming data with DuckDB. Registering DataFrame as table '{self.table_name}'.")

        try:
            # Establish a connection to an in-memory DuckDB database
            con = duckdb.connect(database=':memory:', read_only=False)

            # Register the pandas DataFrame as a virtual table in DuckDB
            # This is a zero-copy operation, making it extremely fast.
            con.register(self.table_name, source_df)

            # Execute the transformation query
            result_df = con.execute(query).fetch_df()

        except Exception as e:
            # Catch DuckDB-specific errors and other potential issues
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            # Ensure the connection is closed
            if 'con' in locals() and con:
                con.close()

        print(f"Transformation complete. Resulting DataFrame has {len(result_df)} rows and {len(result_df.columns)} columns.")
        
        # Create a new DataContainer for the output
        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duckdb_transformed'] = True

        return output_container