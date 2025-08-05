# scripts/plugins/transformers/with_duckdb.py

import duckdb
from typing import Dict, Any, Optional

from .base import BaseTransformer
from scripts.core.data_container.container import DataContainer

class DuckDBTransformer(BaseTransformer):
    """
    Transforms data using SQL queries powered by DuckDB.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.query = self.params.get("query")
        self.query_file = self.params.get("query_file")
        self.table_name = self.params.get("table_name", "source_data")
        if not self.query and not self.query_file:
            raise ValueError("DuckDBTransformer requires either a 'query' or 'query_file' parameter.")

    def _get_query(self) -> str:
        if self.query:
            return self.query
        try:
            with open(self.query_file, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found: {self.query_file}")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("DuckDBTransformer requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DuckDBTransformer received a DataContainer with no DataFrame. Skipping.")
            return data

        source_df = data.data
        query = self._get_query()

        print(f"Transforming data with DuckDB. Registering DataFrame as table '{self.table_name}'.")

        try:
            con = duckdb.connect(database=':memory:', read_only=False)
            con.register(self.table_name, source_df)
            result_df = con.execute(query).fetch_df()
        except Exception as e:
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con:
                con.close()

        print(f"Transformation complete. Resulting DataFrame has {len(result_df)} rows and {len(result_df.columns)} columns.")

        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duckdb_transformed'] = True

        return output_container