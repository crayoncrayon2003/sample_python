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
    step_name: str,
    plugin_name: str,
    params: Dict[str, Any],
    inputs: Dict[str, Optional[DataContainer]]
) -> Optional[DataContainer]:
    """ A Prefect task that executes a single plugin step. """
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    return step_executor.execute_step(step_config, inputs)


@flow(name="ETL1 - CSV to Parquet (pluggy-based)", log_prints=True)
def etl1_main_flow():
    """
    Defines and runs the ETL1 pipeline using the pluggy-based framework.
    """

    # 1. Extract CSV
    extract_result = execute_step_task.submit(
        step_name="extract_csv",
        plugin_name="from_local_file",
        params={"path": project_root / "ETL1/01_data/input/source_data.csv"},
        inputs={}
    )

    # 2. Validate Raw Data
    validate_raw_result = execute_step_task.submit(
        step_name="validate_raw_data",
        plugin_name="data_quality",
        params={"rules": [
            {"column": "product_id", "type": "not_null"},
            {"column": "product_id", "type": "is_unique"},
            {"column": "price", "type": "in_range", "min": 0},
            {"column": "quantity", "type": "in_range", "min": 0},
        ]},
        inputs={"input_data": extract_result}
    )

    # 3. Add Timestamp
    add_timestamp_result = execute_step_task.submit(
        step_name="add_timestamp",
        plugin_name="with_duckdb",
        params={
            "query_file": project_root / "ETL1/05_queries/add_timestamp.sql",
            "table_name": "sales_data",
        },
        inputs={"input_data": validate_raw_result}
    )

    # 4. Validate Transformed Data
    validate_transformed_result = execute_step_task.submit(
        step_name="validate_transformed_data",
        plugin_name="data_quality",
        params={"rules": [
            {"column": "product_id", "type": "not_null"},
            {"column": "processing_timestamp", "type": "not_null"},
        ]},
        inputs={"input_data": add_timestamp_result}
    )

    # 5. Load to Parquet
    execute_step_task.submit(
        step_name="load_to_parquet",
        plugin_name="to_local_file",
        params={
            "output_path": project_root / "ETL1/01_data/output/processed_sales.parquet",
            "format": "parquet",
        },
        inputs={"input_data": validate_transformed_result}
    )

if __name__ == "__main__":
    etl1_main_flow()