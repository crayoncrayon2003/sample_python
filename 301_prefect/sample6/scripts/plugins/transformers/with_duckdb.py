# scripts/plugins/transformers/with_duckdb.py

import duckdb
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_duckdb"

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
        query = params.get("query")
        query_file = params.get("query_file")
        table_name = params.get("table_name", "source_data")
        if not query and not query_file:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'query' or 'query_file'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None:
            print("Warning: DuckDBTransformer received no DataFrame.")
            return data

        source_df = data.data
        sql_query = self._get_query(params)
        print(f"Transforming data with DuckDB, table name: '{table_name}'.")

        try:
            con = duckdb.connect(database=':memory:', read_only=False)
            con.register(table_name, source_df)
            result_df = con.execute(sql_query).fetch_df()
        except Exception as e:
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con: con.close()

        print(f"Transformation complete. Result shape: {result_df.shape}")
        output_container = DataContainer(data=result_df)
        output_container.metadata = data.metadata.copy()
        output_container.file_paths = data.file_paths.copy()
        output_container.metadata['duckdb_transformed'] = True
        return output_container