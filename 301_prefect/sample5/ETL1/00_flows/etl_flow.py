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

# --- フレームワークの部品を直接インポート ---
from scripts.core.data_container.container import DataContainer
from scripts.core.pipeline.step_executor import StepExecutor

# --- StepExecutorを一度だけインスタンス化 ---
# このインスタンスを各タスクで共有する
step_executor = StepExecutor()

# --- 各ステップを個別のPrefectタスクとして定義するヘルパー関数 ---
@task
def execute_step_task(
    step_config: Dict[str, Any],
    inputs: Dict[str, Optional[DataContainer]]
) -> Optional[DataContainer]:
    """ A Prefect task that executes a single step using the StepExecutor. """
    return step_executor.execute_step(step_config, inputs)

# --- ETL1のメインフロー ---
@flow(name="ETL1 - CSV to Parquet (Prefect Native)", log_prints=True)
def etl1_main_flow():
    """
    Defines and runs the ETL1 pipeline using a Prefect-native approach.
    """
    
    # 1. extract_csv タスクを実行
    extract_result = execute_step_task.submit(
        step_config={
            "name": "extract_csv", "plugin": "from_local_file",
            "params": {"path": project_root / "ETL1/01_data/input/source_data.csv"}
        },
        inputs={} # Extractorなので入力は空
    )

    # 2. validate_raw_data タスクを実行
    #    Prefectは、extract_resultが完了するのを自動的に待つ
    validate_raw_result = execute_step_task.submit(
        step_config={
            "name": "validate_raw_data", "plugin": "data_quality",
            "params": {"rules": [
                {"column": "product_id", "type": "not_null"}, {"column": "product_id", "type": "is_unique"},
                {"column": "price", "type": "in_range", "min": 0}, {"column": "quantity", "type": "in_range", "min": 0},
            ]}
        },
        inputs={"input_data": extract_result}
    )

    # 3. add_timestamp タスクを実行
    add_timestamp_result = execute_step_task.submit(
        step_config={
            "name": "add_timestamp", "plugin": "with_duckdb",
            "params": {
                "query_file": project_root / "ETL1/05_queries/add_timestamp.sql",
                "table_name": "sales_data",
            }
        },
        inputs={"input_data": validate_raw_result}
    )

    # 4. validate_transformed_data タスクを実行
    validate_transformed_result = execute_step_task.submit(
        step_config={
            "name": "validate_transformed_data", "plugin": "data_quality",
            "params": {"rules": [
                {"column": "product_id", "type": "not_null"},
                {"column": "processing_timestamp", "type": "not_null"},
            ]}
        },
        inputs={"input_data": add_timestamp_result}
    )
    
    # 5. load_to_parquet タスクを実行
    execute_step_task.submit(
        step_config={
            "name": "load_to_parquet", "plugin": "to_local_file",
            "params": {
                "output_path": project_root / "ETL1/01_data/output/processed_sales.parquet",
                "format": "parquet",
            }
        },
        inputs={"input_data": validate_transformed_result}
    )

if __name__ == "__main__":
    etl1_main_flow()