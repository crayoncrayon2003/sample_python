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

# --- フレームワークの部品をインポート ---
from scripts.core.data_container.container import DataContainer
from scripts.core.pipeline.step_executor import StepExecutor

# --- StepExecutorを一度だけインスタンス化 ---
step_executor = StepExecutor()

# --- 再利用可能なタスク定義 ---
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

# --- ETL3のメインフロー：分岐と結合 ---
@flow(name="ETL3 - Fork and Join Example", log_prints=True)
def etl3_main_flow():
    """
    Demonstrates a fork-join DAG pattern using the pluggy-based framework.
    """

    # --- (A) データを準備 ---
    # このタスクの結果が、2つのブランチで共有される
    prep_data_result = execute_step_task.submit(
        step_name="prepare_data",
        plugin_name="from_local_file",
        params={"path": project_root / "ETL1/01_data/input/source_data.csv"},
        inputs={}
    )

    # --- (B) 分岐1：変換1 -> 保存1 ---
    # `prep_data_result` を入力として使用
    transform1_result = execute_step_task.submit(
        step_name="transform_1_add_status_A",
        plugin_name="with_duckdb",
        params={"query": "SELECT *, 'branch_A' as branch_status FROM source_data"},
        inputs={"input_data": prep_data_result}
    )
    # 保存タスク。このタスクは下流では使わないので、結果を保存する変数は不要
    execute_step_task.submit(
        step_name="save_branch_1_output",
        plugin_name="to_local_file",
        params={"output_path": project_root / "ETL3/01_data/output/output_1.parquet"},
        inputs={"input_data": transform1_result}
    )

    # --- (C) 分岐2：変換2 -> 保存2 ---
    # `prep_data_result` を入力として使用 (ここが分岐点)
    transform2_result = execute_step_task.submit(
        step_name="transform_2_add_status_B",
        plugin_name="with_duckdb",
        params={"query": "SELECT *, 'branch_B' as branch_status FROM source_data"},
        inputs={"input_data": prep_data_result}
    )
    execute_step_task.submit(
        step_name="save_branch_2_output",
        plugin_name="to_local_file",
        params={"output_path": project_root / "ETL3/01_data/output/output_2.csv"},
        inputs={"input_data": transform2_result}
    )

    # --- (D) 結合：変換3 -> 保存3 ---
    # `transform1_result` と `transform2_result` の両方の完了を待つ
    join_result = execute_step_task.submit(
        step_name="join_transformations",
        plugin_name="dataframe_joiner",
        params={"pandas_options": {"ignore_index": True}},
        # ★★★ 複数の入力を異なるキーで指定 ★★★
        inputs={
            "data_from_branch1": transform1_result,
            "data_from_branch2": transform2_result
        }
    )

    # 結合した結果をさらに変換
    final_transform_result = execute_step_task.submit(
        step_name="final_transformation_add_length",
        plugin_name="with_duckdb",
        params={"query": "SELECT *, LENGTH(product_name) as name_length FROM source_data"},
        inputs={"input_data": join_result}
    )

    execute_step_task.submit(
        step_name="save_final_joined_output",
        plugin_name="to_local_file",
        params={"output_path": project_root / "ETL3/01_data/output/final_output.csv"},
        inputs={"input_data": final_transform_result}
    )

if __name__ == "__main__":
    etl3_main_flow()