# ETL1/00_flows/etl_flow.py
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


@flow(name="ETL1 - CSV to Parquet and Report")
def etl1_flow():
    """
    Orchestrates the ETL process for ETL1.

    This flow performs the following steps:
    1.  Loads configuration from `config.yml`.
    2.  Extracts data from a source CSV file.
    3.  (Optional) Transforms the data using a DuckDB SQL query.
    4.  Loads the transformed structured data to a final Parquet file.
    5.  (Optional) Generates a text-based report using a Jinja2 template.
    6.  Loads the generated report to a file.
    """
    logger = get_run_logger()
    logger.info("Starting ETL1 flow...")

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
    final_parquet_path = etl_root / data_conf.get("output_path", "")

    # --- 2. Extract ---
    # Convert the source CSV to an intermediate Parquet file.
    extracted_file_path = extract_from_local_file.submit(
        input_path=input_file_path,
        working_dir=working_dir_path
    )

    # This variable will hold the path to the latest version of our structured data.
    latest_structured_data = extracted_file_path

    # --- 3. Transform with DuckDB (Optional) ---
    if duckdb_conf:
        query_file_path = etl_root / duckdb_conf.get("query_path", "")
        # The DuckDB task depends on the completion of the extraction task.
        latest_structured_data = transform_with_duckdb.submit(
            input_path=latest_structured_data,
            working_dir=working_dir_path,
            query_path=query_file_path,
            # Pass any other parameters from config directly to the template
            **{k: v for k, v in duckdb_conf.items() if k != "query_path"},
        )

    # --- 4. Load Final Parquet ---
    # This task depends on the completion of all previous transformations.
    load_to_local_file.submit(
        input_path=latest_structured_data,
        output_path=final_parquet_path
    )
    logger.info(f"Main data pipeline finished. Final Parquet at: {final_parquet_path}")

    # --- 5. Generate Report with Jinja2 (Optional) ---
    if jinja_conf:
        template_path = etl_root / jinja_conf.get("template_path", "")
        output_filename = jinja_conf.get("output_filename", "report.txt")

        # The report generation depends on the final state of the structured data.
        generated_report_path = transform_with_jinja2.submit(
            input_path=latest_structured_data,
            working_dir=working_dir_path,
            template_path=template_path,
            output_filename=output_filename,
            # Pass any other parameters from config directly to the template
            **{k: v for k, v in jinja_conf.items() if k not in ["template_path", "output_filename"]},
        )

        # --- 6. Load Report ---
        report_output_path = final_parquet_path.parent / output_filename
        load_to_local_file.submit(
            input_path=generated_report_path,
            output_path=report_output_path
        )
        logger.info(f"Report generation finished. Final report at: {report_output_path}")

    logger.info("ETL1 flow finished successfully.")


if __name__ == "__main__":
    # This allows for direct execution of the flow for testing purposes.
    etl1_flow()