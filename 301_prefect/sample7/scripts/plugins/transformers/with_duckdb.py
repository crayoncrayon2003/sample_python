# scripts/plugins/transformers/with_duckdb.py

import duckdb
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer:
    """
    (File-based) Transforms data in a file using a SQL query powered by DuckDB.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_duckdb"

    def _get_query(self, params: Dict[str, Any]) -> str:
        if params.get("query"):
            return params.get("query")
        query_file_path = params.get("query_file")
        if not query_file_path:
            raise ValueError("DuckDBTransformer requires either 'query' or 'query_file'.")
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
        sql_query = self._get_query(params)
        input_path_str = params.get("input_path")

        if not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_path'.")

        print(f"Executing DuckDB transformation. Output path: '{output_path}'.")

        try:
            con = duckdb.connect(database=':memory:', read_only=False)

            if input_path_str:
                input_path = Path(input_path_str)
                if not input_path.exists():
                    raise FileNotFoundError(f"Input file not found at: {input_path}")

                table_name = params.get("table_name", "source_data")
                con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{str(input_path)}');")
                print(f"Registered input file '{input_path.name}' as table '{table_name}'.")

            result_df = con.execute(sql_query).fetch_df()
        except Exception as e:
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con:
                con.close()

        print(f"Transformation complete. Result has {len(result_df)} rows. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_parquet(output_path, index=False)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['duckdb_transformed'] = True
        return output_container