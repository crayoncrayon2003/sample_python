# ETL2/00_flows/etl_flow.py

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
step_executor = StepExecutor()

# --- 再利用可能なタスク定義 ---
@task
def execute_step_task(
    step_config: Dict[str, Any],
    inputs: Dict[str, Optional[DataContainer]]
) -> Optional[DataContainer]:
    """ A Prefect task that executes a single step using the StepExecutor. """
    return step_executor.execute_step(step_config, inputs)

# --- ETL2のメインフロー ---
@flow(name="ETL2 - CSV to NGSI-v2 (DAG-based)", log_prints=True)
def etl2_main_flow():
    """
    Defines and runs the ETL2 pipeline using a Prefect-native DAG approach.
    """

    # 1. Extract
    extract_result = execute_step_task.submit(
        step_config={
            "name": "extract_measurements_csv",
            "plugin": "from_local_file",
            "params": {"path": project_root / "ETL2/01_data/input/measurements_ok.csv"}
            # "params": {"path": project_root / "ETL2/01_data/input/measurements_ng.csv"}
        },
        inputs={}
    )

    # 2. Pivot (Transform)
    pivot_result = execute_step_task.submit(
        step_config={
            "name": "pivot_raw_data",
            "plugin": "with_duckdb",
            "params": {
                "query_file": project_root / "ETL2/05_queries/00_pivot_data.sql",
                "table_name": "measurements",
            }
        },
        inputs={"input_data": extract_result}
    )

    # 3. Validate
    validate_result = execute_step_task.submit(
        step_config={
            "name": "validate_pivoted_data",
            "plugin": "business_rules",
            "params": {"rules": [
                {"name": "Temp out of range", "expression": "temperature < -50 or temperature > 100"},
                {"name": "Humidity out of range", "expression": "humidity < 0 or humidity > 100"},
                {"name": "Timestamp missing", "expression": "timestamp.isnull()"},
            ]}
        },
        inputs={"input_data": pivot_result}
    )

    # 4. Structure for NGSI (Transform)
    structure_result = execute_step_task.submit(
        step_config={
            "name": "structure_data_for_ngsi",
            "plugin": "with_duckdb",
            "params": {
                "query_file": project_root / "ETL2/05_queries/02_structure_to_ngsi.sql",
                "table_name": "measurements",
            }
        },
        inputs={"input_data": validate_result}
    )

    # 5. Transform to NGSI JSON
    to_ngsi_result = execute_step_task.submit(
        step_config={
            "name": "transform_to_ngsi_entities",
            "plugin": "to_ngsi",
            "params": {
                "template_path": project_root / "ETL2/04_templates/to_ngsiv2.json.j2",
                "entity_type": "Measurement",
                "id_prefix": "urn:ngsi-ld:Measurement:",
                "id_column": "sensor_id",
                "output_column_name": "ngsi_entity_json",
            }
        },
        inputs={"input_data": structure_result}
    )

    # 6. Validate NGSI JSON
    validate_ngsi_result = execute_step_task.submit(
        step_config={
            "name": "validate_ngsi_output",
            "plugin": "ngsi_validator",
            "params": {
                "target_column": "ngsi_entity_json",
                "ngsi_version": "v2",
            }
        },
        inputs={"input_data": to_ngsi_result}
    )

    # 7. Load to file
    execute_step_task.submit(
        step_config={
            "name": "load_to_jsonl_file",
            "plugin": "to_local_file",
            "params": {
                "output_path": project_root / "ETL2/01_data/output/ngsi_entities.json",
                "format": "json",
                "pandas_options": { "orient": "records", "lines": True },
            }
        },
        inputs={"input_data": validate_ngsi_result}
    )

if __name__ == "__main__":
    etl2_main_flow()