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
    step_config: Dict[str, Any],
    inputs: Dict[str, Optional[DataContainer]]
) -> Optional[DataContainer]:
    return step_executor.execute_step(step_config, inputs)

@flow(name="ETL3 - Fork and Join Example", log_prints=True)
def etl3_main_flow():
    """
    Demonstrates a fork-join DAG pattern.
    1. Prepare data.
    2. Fork into two parallel branches (transform/save 1 & 2).
    3. Join the results of the transformations.
    4. Perform a final transformation and save.
    """

    # --- (A) データを準備 ---
    prep_data_result = execute_step_task.submit(
        step_config={"name": "prepare_data", "plugin": "from_local_file",
                     "params": {"path": project_root / "ETL3/01_data/input/source_data.csv"}},
        inputs={}
    )

    # --- (B) 分岐1：変換1 -> 保存1 ---
    # `prep_data_result` を入力として使用
    transform1_result = execute_step_task.submit(
        step_config={"name": "transform_1_add_status_A", "plugin": "with_duckdb",
                     "params": {"query": "SELECT *, 'status_A' as status FROM source_data"}},
        inputs={"input_data": prep_data_result}
    )
    execute_step_task.submit(
        step_config={"name": "save_1", "plugin": "to_local_file",
                     "params": {"output_path": project_root / "ETL3/01_data/output/output_1.parquet"}},
        inputs={"input_data": transform1_result}
    )

    # --- (C) 分岐2：変換2 -> 保存2 ---
    # `prep_data_result` を入力として使用 (ここが分岐点)
    transform2_result = execute_step_task.submit(
        step_config={"name": "transform_2_add_status_B", "plugin": "with_duckdb",
                     "params": {"query": "SELECT *, 'status_B' as status FROM source_data"}},
        inputs={"input_data": prep_data_result}
    )
    execute_step_task.submit(
        step_config={"name": "save_2", "plugin": "to_local_file",
                     "params": {"output_path": project_root / "ETL3/01_data/output/output_2.parquet"}},
        inputs={"input_data": transform2_result}
    )

    # --- (D) 結合：変換3 -> 保存3 ---
    # `transform1_result` と `transform2_result` の両方が完了するのを待つ
    join_result = execute_step_task.submit(
        step_config={"name": "join_transformations", "plugin": "dataframe_joiner",
                     "params": {"pandas_options": {"ignore_index": True}}},
        # ★★★ 複数の入力を異なるキーで指定 ★★★
        inputs={
            "source1": transform1_result,
            "source2": transform2_result
        }
    )

    # 結合した結果をさらに変換
    final_transform_result = execute_step_task.submit(
        step_config={"name": "final_transformation", "plugin": "with_duckdb",
                     "params": {"query": "SELECT *, LENGTH(product_name) as name_length FROM source_data"}},
        inputs={"input_data": join_result}
    )

    execute_step_task.submit(
        step_config={"name": "save_final", "plugin": "to_local_file",
                     "params": {"output_path": project_root / "ETL3/01_data/output/final_output.csv"}},
        inputs={"input_data": final_transform_result}
    )


if __name__ == "__main__":
    etl3_main_flow()