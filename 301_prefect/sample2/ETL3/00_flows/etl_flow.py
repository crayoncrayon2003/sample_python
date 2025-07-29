# ETL3/00_flows/etl_flow.py
import sys
from pathlib import Path

from prefect import flow, get_run_logger

# --- Python Path Setup ---
# Add the project root to the Python path to allow importing from 'scripts'.
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# --- Common Task and Utility Imports ---
from scripts.tasks.extract.from_local_json import extract_from_local_json
from scripts.tasks.load.to_local_file import load_to_local_file
from scripts.tasks.transform.with_duckdb import transform_with_duckdb
from scripts.tasks.transform.with_jinja2 import transform_with_jinja2
from scripts.tasks.validation.validate_json import validate_json_with_schema
from scripts.utils import load_config


@flow(name="ETL3 - Validated JSON to NGSI-v2")
def etl3_flow():
    """
    Orchestrates the ETL process for ETL3 with data validation.

    This flow performs the following steps:
    1.  Loads configuration from `config.yml`.
    2.  Validates the source JSON file against an input schema.
    3.  Extracts data from the validated JSON file.
    4.  (Optional) Sequentially applies a list of DuckDB SQL transformations.
    5.  Generates a final JSON file in NGSI-v2 format using a Jinja2 template.
    6.  Validates the generated JSON file against an output schema.
    7.  Loads the validated JSON to the final output destination.
    """
    logger = get_run_logger()
    logger.info("Starting ETL3 flow with validation...")

    # Define the root directory for this specific ETL pipeline
    etl_root = Path(__file__).parent.parent

    # --- 1. Load Configuration ---
    config = load_config(etl_root / "config.yml")
    data_conf = config.get("data", {})
    validation_conf = config.get("validation", {})
    duckdb_conf = config.get("transform_duckdb", {})
    jinja_conf = config.get("transform_jinja", {})

    # Resolve file paths from configuration
    input_file_path = etl_root / data_conf.get("input_path", "")
    working_dir_path = etl_root / data_conf.get("working_dir", "01_data/working")
    final_output_path = etl_root / data_conf.get("output_path", "")
    input_schema_path = etl_root / validation_conf.get("input_schema_path", "")
    output_schema_path = etl_root / validation_conf.get("output_schema_path", "")

    # --- 2. Input Validation ---
    # This is a control dependency; it must succeed before extraction starts.
    input_validation_future = validate_json_with_schema.submit(
        data_path=input_file_path, schema_path=input_schema_path
    )

    # --- 3. Extract ---
    # The 'wait_for' ensures this task only runs after a successful validation.
    extracted_file_path = extract_from_local_json.submit(
        input_path=input_file_path,
        working_dir=working_dir_path,
        wait_for=[input_validation_future],
    )

    # This variable will hold the path to the latest version of the structured data.
    structured_data_path = extracted_file_path

    # --- 4. Transform with DuckDB (Chained, Optional) ---
    if duckdb_conf and "query_paths" in duckdb_conf:
        query_paths = duckdb_conf.get("query_paths", [])
        for query_file in query_paths:
            query_path = etl_root / query_file
            logger.info(f"Applying SQL transformation: {query_path.name}")
            structured_data_path = transform_with_duckdb.submit(
                input_path=structured_data_path,
                working_dir=working_dir_path,
                query_path=query_path,
                **{k: v for k, v in duckdb_conf.items() if k != "query_paths"},
            )

    # --- 5. Transform with Jinja2 ---
    # This depends on the final result of the DuckDB transformation chain.
    generated_json_path = transform_with_jinja2.submit(
        input_path=structured_data_path,
        working_dir=working_dir_path,
        template_path=etl_root / jinja_conf.get("template_path", ""),
        output_filename=jinja_conf.get("output_filename", "output.json"),
        **{
            k: v
            for k, v in jinja_conf.items()
            if k not in ["template_path", "output_filename"]
        },
    )

    # --- 6. Output Validation ---
    # This control dependency ensures the generated data is valid before loading.
    output_validation_future = validate_json_with_schema.submit(
        data_path=generated_json_path, schema_path=output_schema_path
    )

    # --- 7. Load Final JSON ---
    # The 'wait_for' ensures this task only runs after successful output validation.
    load_to_local_file.submit(
        input_path=generated_json_path,
        output_path=final_output_path,
        wait_for=[output_validation_future],
    )

    logger.info(f"ETL3 flow finished successfully. Final output will be at: {final_output_path}")


if __name__ == "__main__":
    etl3_flow()