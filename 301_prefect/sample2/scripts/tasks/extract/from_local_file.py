# scripts/tasks/extract/from_local_file.py
from datetime import datetime
from pathlib import Path

import duckdb
from prefect import task


@task
def extract_from_local_file(input_path: Path, working_dir: Path) -> str:
    """
    Reads a local CSV file, converts it to Parquet format, and saves it
    to a working directory.

    This task serves as the initial step for CSV-based ETL pipelines,
    standardizing the input data into the efficient Parquet format for
    downstream processing.

    Args:
        input_path: The path to the source CSV file.
        working_dir: The directory to save the intermediate Parquet file.

    Returns:
        The path to the newly created intermediate Parquet file as a string.
    """
    print(f"Extract task: Reading data from {input_path}...")

    # Define a unique name for the intermediate file
    output_filename = f"extracted_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    output_path = working_dir / output_filename

    # Ensure the working directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use DuckDB to efficiently read the CSV and write to Parquet
    con = duckdb.connect(database=':memory:')
    try:
        query = f"COPY (SELECT * FROM read_csv_auto('{str(input_path)}')) TO '{str(output_path)}' (FORMAT 'PARQUET');"
        con.execute(query)
    finally:
        con.close()

    print(f"Extraction successful: Intermediate data saved to {output_path}")
    return str(output_path)