# ETL2/00_flows/etl_flow.py
import sys
from pathlib import Path

from prefect import flow, get_run_logger

# --- Python Path Setup ---
# Add the project root to the Python path to allow importing from 'scripts'.
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# --- Common Task and Utility Imports ---
from scripts.tasks.extract.from_local_file import extract_from_local_file
from scripts.tasks.load.to_local_file import load_to_local_file
from scripts.tasks.transform.with_duckdb import transform_with_duckdb
from scripts.tasks.transform.with_jinja2 import transform_with_jinja2
from scripts.utils import load_config


@flow(name="ETL2 - CSV to NGSI-v2 JSON")
def etl2_flow():
    """
    Orchestrates the ETL process for ETL2.

    This flow performs the following steps:
    1.  Loads configuration from `config.yml`.
    2.  Extracts data from a source CSV file.
    3.  Sequentially applies a list of DuckDB SQL transformations defined in the config.
        - Step 3a: Validates source data.
        - Step 3b: Structures the data into the NGSI format.
    4.  Generates a final JSON file in NGSI-v2 format using a Jinja2 template.
    5.  Loads the generated JSON to the final output destination.
    """
    logger = get_run_logger()
    logger.info("Starting ETL2 flow...")

    # Define the root directory for this specific ETL pipeline
    etl_root = Path(__file__).parent.parent

    # --- 1. Load Configuration ---
    config = load_config(etl_root / "config.yml")
    data_conf = config.get("data", {})
    duckdb_conf = config.get("transform_duckdb", {})
    jinja_conf = config.get("transform_jinja", {})

    # Resolve file paths relative to the ETL root
    input_file_path = etl_root / data_conf.get("input_path", "")
    working_dir_path = etl_root / data_conf.get("working_dir", "01_data/working")
    final_output_path = etl_root / data_conf.get("output_path", "")

    # --- 2. Extract ---
    # Convert the source CSV to an intermediate Parquet file.
    extracted_file_path = extract_from_local_file.submit(
        input_path=input_file_path,
        working_dir=working_dir_path,
    )

    # This variable will hold the path to the latest version of the structured data.
    # It starts with the output of the extraction task.
    structured_data_path = extracted_file_path

    # --- 3. Transform with DuckDB (Chained) ---
    # Check if 'query_paths' list is defined in the configuration.
    if duckdb_conf and "query_paths" in duckdb_conf:
        query_paths = duckdb_conf.get("query_paths", [])
        
        # Loop through the list of SQL files and apply them sequentially.
        # Each task in the loop will depend on the result of the previous one.
        for query_file in query_paths:
            query_path = etl_root / query_file
            logger.info(f"Applying SQL transformation: {query_path.name}")
            
            # Re-assign the variable to chain the dependency.
            # The next loop iteration (or subsequent task) will wait for this to complete.
            structured_data_path = transform_with_duckdb.submit(
                input_path=structured_data_path, # Use the result from the previous step
                working_dir=working_dir_path,
                query_path=query_path,
                # Pass all other params from the 'transform_duckdb' config section
                # to the SQL template, excluding 'query_paths' itself.
                **{k: v for k, v in duckdb_conf.items() if k != "query_paths"},
            )

    # --- 4. Transform with Jinja2 (Formatting to NGSI-v2) ---
    # This task now correctly depends on the final result of the DuckDB transformations
    # because it uses the 'structured_data_path' variable, which was updated in the loop.
    generated_json_path = transform_with_jinja2.submit(
        input_path=structured_data_path,
        working_dir=working_dir_path,
        template_path=etl_root / jinja_conf.get("template_path", ""),
        output_filename=jinja_conf.get("output_filename", "output.json"),
        # Pass all other params from the 'transform_jinja' config section
        # dynamically to the Jinja2 template.
        **{
            k: v
            for k, v in jinja_conf.items()
            if k not in ["template_path", "output_filename"]
        },
    )

    # --- 5. Load Final JSON ---
    # This task depends on the completion of the Jinja2 transformation.
    load_to_local_file.submit(
        input_path=generated_json_path,
        output_path=final_output_path
    )

    logger.info(f"ETL2 flow finished successfully. Final output will be at: {final_output_path}")


if __name__ == "__main__":
    # Allows for direct execution of the flow for testing.
    # Assumes 'measurements.csv' is manually placed in the input directory.
    etl2_flow()