# ETL1/00_flows/etl_flow.py

import os
import sys
from pathlib import Path

# --- Add project root to Python path ---
try:
    project_root = Path(__file__).resolve().parents[2]
    if "scripts" not in os.listdir(project_root): raise FileNotFoundError
except (IndexError, FileNotFoundError):
    project_root = Path(os.getcwd())
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# -----------------------------------------

from prefect import flow, task
from typing import Dict, Any, Optional

from scripts.core.data_container.container import DataContainer
from scripts.core.pipeline.step_executor import StepExecutor

step_executor = StepExecutor()

@task
def execute_step_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    inputs = inputs or {}
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    return step_executor.execute_step(step_config, inputs)

@flow(name="ETL1 - CSV to Parquet (Final Design)", log_prints=True)
def etl1_main_flow():
    """
    Defines and runs ETL1 with the final, robust file-passing architecture.
    """

    # --- 1. Define paths ---
    working_dir = project_root / "ETL1/01_data/working"
    source_file = project_root / "ETL1/01_data/input/source_data.csv"
    staged_file = working_dir / "01_staged.parquet"
    validated_raw_file = working_dir / "02_validated_raw.parquet"
    transformed_file = working_dir / "03_transformed.parquet"
    validated_transformed_file = working_dir / "04_validated_transformed.parquet"
    final_output_file = project_root / "ETL1/01_data/output/processed_sales.parquet"

    # --- 2. Define the pipeline DAG ---

    # Step 1: Extract (No `inputs`)
    extract_result = execute_step_task.submit(
        step_name="extract_csv_to_parquet",
        plugin_name="from_local_file",
        params={
            "source_path": source_file,
            "output_path": staged_file
        }
    )

    # Step 2: Validate Raw Data (`inputs` defines dependency and data source)
    validate_raw_result = execute_step_task.submit(
        step_name="validate_raw_data",
        plugin_name="data_quality",
        params={
            "output_path": validated_raw_file,
            "rules": [
                {"column": "product_id", "type": "not_null"},
                {"column": "product_id", "type": "is_unique"},
                {"column": "price", "type": "in_range", "min": 0},
                {"column": "quantity", "type": "in_range", "min": 0},
            ]
        },
        inputs={"input_data": extract_result} # This implies input_path
    )

    # Step 3: Add Timestamp
    add_timestamp_result = execute_step_task.submit(
        step_name="add_timestamp",
        plugin_name="with_duckdb",
        params={
            "output_path": transformed_file,
            "query_file": project_root / "ETL1/05_queries/add_timestamp.sql",
            "table_name": "sales_data",
        },
        inputs={"input_data": validate_raw_result}
    )

    # Step 4: Validate Transformed Data
    validate_transformed_result = execute_step_task.submit(
        step_name="validate_transformed_data",
        plugin_name="data_quality",
        params={
            "output_path": validated_transformed_file,
            "rules": [
                {"column": "product_id", "type": "not_null"},
                {"column": "processing_timestamp", "type": "not_null"},
            ]
        },
        inputs={"input_data": add_timestamp_result}
    )

    # Step 5: Load to Final Destination
    execute_step_task.submit(
        step_name="load_to_final_parquet",
        plugin_name="to_local_file",
        params={
            "output_path": final_output_file
        },
        inputs={"input_data": validate_transformed_result}
    )

if __name__ == "__main__":
    etl1_main_flow()