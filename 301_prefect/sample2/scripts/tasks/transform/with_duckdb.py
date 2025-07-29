# scripts/tasks/transform/with_duckdb.py
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import duckdb
from prefect import task

from scripts.utils import load_sql_template


@task
def transform_with_duckdb(
    input_path: str, working_dir: Path, query_path: Path, **query_params: Any
) -> str:
    """
    Transforms data using a DuckDB query loaded from an external SQL file.

    This task reads an intermediate Parquet file, applies a SQL transformation,
    and writes the result to a new Parquet file in the working directory.
    The input data is made available within the query as a view named 'source'.

    Args:
        input_path: The path to the source Parquet file.
        working_dir: The directory to save the transformed Parquet file.
        query_path: The path to the .sql file containing the transformation logic.
        **query_params: Keyword arguments passed to the SQL template for rendering.
                        This allows for dynamic query generation.

    Returns:
        The path to the newly created transformed Parquet file as a string.
    """
    print(f"DuckDB Transform: Applying '{query_path.name}' to '{Path(input_path).name}'...")

    # Define a unique name for the transformed file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"transformed_duckdb_{timestamp}.parquet"
    output_path = working_dir / output_filename

    # Render the SQL query using Jinja2, allowing for dynamic parameters
    rendered_sql = load_sql_template(query_path, **query_params)
    print(f"Executing Query:\n---\n{rendered_sql}\n---")

    # Connect to an in-memory DuckDB database
    con = duckdb.connect(database=":memory:")
    try:
        # Create a view named 'source' from the input Parquet file.
        # This allows the SQL query to reference the input data idempotently.
        con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM read_parquet('{input_path}');")

        # Execute the main transformation query and save the result to a new Parquet file.
        # The query is wrapped in a COPY statement for direct output.
        con.execute(f"COPY ({rendered_sql}) TO '{str(output_path)}' (FORMAT 'PARQUET');")
    finally:
        # Ensure the database connection is always closed
        con.close()

    print(f"DuckDB Transform successful: Data saved to {output_path}")
    return str(output_path)