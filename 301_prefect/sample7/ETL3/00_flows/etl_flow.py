# ETL3/00_flows/etl_flow.py

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

@flow(name="ETL3 - Fork and Join (File-based)", log_prints=True)
def etl3_main_flow():
    """
    Demonstrates a fork-join DAG pattern using the file-passing architecture.
    """

    # --- 1. Define all file paths ---
    working_dir = project_root / "ETL3/01_data/working"
    output_dir = project_root / "ETL3/01_data/output"

    # (A)
    source_file = project_root / "ETL1/01_data/input/source_data.csv"
    prepared_file = working_dir / "01_prepared.parquet"

    # (B) Branch 1
    transformed_file_A = working_dir / "02_transformed_A.parquet"
    final_output_1 = output_dir / "output_1.parquet"

    # (C) Branch 2
    transformed_file_B = working_dir / "02_transformed_B.parquet"
    final_output_2 = output_dir / "output_2.csv"

    # (D) After Join
    joined_file = working_dir / "03_joined.parquet"
    final_transformed_file = working_dir / "04_final_transformed.parquet"
    final_output_3 = output_dir / "final_output.csv"

    # --- 2. Define the pipeline DAG ---

    # (A) Prepare Data
    prep_data_result = execute_step_task.submit(
        step_name="prepare_data", plugin_name="from_local_file",
        params={"source_path": source_file, "output_path": prepared_file}
    )

    # --- (B) Branch 1 ---
    transform1_result = execute_step_task.submit(
        step_name="transform_1_add_status_A", plugin_name="with_duckdb",
        params={
            "output_path": transformed_file_A,
            "query": "SELECT *, 'branch_A' as branch_status FROM source_data"
        },
        inputs={"input_data": prep_data_result}
    )
    execute_step_task.submit(
        step_name="save_branch_1_output", plugin_name="to_local_file",
        params={"output_path": final_output_1},
        inputs={"input_data": transform1_result}
    )

    # --- (C) Branch 2 ---
    transform2_result = execute_step_task.submit(
        step_name="transform_2_add_status_B", plugin_name="with_duckdb",
        params={
            "output_path": transformed_file_B,
            "query": "SELECT *, 'branch_B' as branch_status FROM source_data"
        },
        inputs={"input_data": prep_data_result} # Fork from the same source
    )
    execute_step_task.submit(
        step_name="save_branch_2_output", plugin_name="to_local_file",
        params={"output_path": final_output_2, "format": "csv"},
        inputs={"input_data": transform2_result}
    )

    # --- (D) Join and Finalize ---
    join_result = execute_step_task.submit(
        step_name="join_branches", plugin_name="with_duckdb",
        params={
            "output_path": joined_file,
            "query": f"""
                SELECT * FROM read_parquet('{str(transformed_file_A)}')
                UNION ALL
                SELECT * FROM read_parquet('{str(transformed_file_B)}');
            """
        },
        # This task depends on both transform tasks to be complete
        inputs={"wait_for_A": transform1_result, "wait_for_B": transform2_result}
    )

    final_transform_result = execute_step_task.submit(
        step_name="final_transformation_add_length", plugin_name="with_duckdb",
        params={
            "output_path": final_transformed_file,
            "query": "SELECT *, LENGTH(product_name) as name_length FROM joined_data",
            "table_name": "joined_data"
        },
        inputs={"input_data": join_result}
    )

    execute_step_task.submit(
        step_name="save_final_joined_output", plugin_name="to_local_file",
        params={
            "output_path": final_output_3,
            "format": "csv"
        },
        inputs={"input_data": final_transform_result}
    )

if __name__ == "__main__":
    etl3_main_flow()