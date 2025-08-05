# ETL1/00_flows/etl_flow.py

import os
import sys
from pathlib import Path

# --- Add project root to Python path ---
try:
    from scripts.utils.file_utils import get_project_root
    project_root = get_project_root()
except (ImportError, FileNotFoundError):
    project_root = Path(__file__).resolve().parents[2]

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# -----------------------------------------

from prefect import flow, task
from scripts.core.orchestrator.flow_executor import FlowExecutor

# taskデコレータを使って、パイプライン全体の実行を1つのPrefectタスクとしてラップします
@task(name="Run ETL1 Pipeline Task")
def run_etl1_pipeline_task(config_path: str):
    """
    This task encapsulates the entire logic of running the ETL1 pipeline.
    """
    try:
        runner = FlowExecutor(config_path=config_path)
        runner.run()
    except Exception as e:
        print(f"ETL1 Pipeline Task failed: {e}")
        raise # Re-raise to make the Prefect task fail

# flowデコレータで、ETL1のメインフローを定義します
@flow(name="ETL1 - CSV to Parquet and Report Flow", log_prints=True)
def etl1_main_flow():
    """
    The main Prefect flow for ETL1.
    """
    print("--- Starting ETL1 Flow ---")
    
    config_file = project_root / "ETL1" / "config.yml"
    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        # Flowを失敗させるために例外を発生させる
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    # 上で定義したタスクを呼び出します
    run_etl1_pipeline_task(config_path=str(config_file))
    
    print("--- ETL1 Flow completed. ---")


if __name__ == "__main__":
    # フローを実行します
    etl1_main_flow()