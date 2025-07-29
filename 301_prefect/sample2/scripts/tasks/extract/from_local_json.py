# scripts/tasks/extract/from_local_json.py
from datetime import datetime
from pathlib import Path

import duckdb
from prefect import task


@task
def extract_from_local_json(input_path: Path, working_dir: Path) -> str:
    """
    Reads a local JSON file, converts it to Parquet format, and saves it
    to a working directory.

    Assumes the JSON file contains a list of objects (or newline-delimited JSON).
    This task standardizes JSON input into the efficient Parquet format for
    downstream processing.

    Args:
        input_path: The path to the source JSON file.
        working_dir: The directory to save the intermediate Parquet file.

    Returns:
        The path to the newly created intermediate Parquet file as a string.
    """
    print(f"JSON Extract task: Reading data from {input_path}...")

    # Define a unique name for the intermediate file
    output_filename = f"extracted_json_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    output_path = working_dir / output_filename

    # Ensure the working directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use DuckDB to efficiently read the JSON and write to Parquet
    con = duckdb.connect(database=':memory:')
    try:
        # read_json_auto with format='auto' can handle arrays of objects
        # or newline-delimited JSON.
        query = f"""
            COPY (
                SELECT * FROM read_json_auto('{str(input_path)}', format='auto')
            ) TO '{str(output_path)}' (FORMAT 'PARQUET');
        """
        con.execute(query)
    finally:
        con.close()

    print(f"JSON Extraction successful: Intermediate data saved to {output_path}")
    return str(output_path)