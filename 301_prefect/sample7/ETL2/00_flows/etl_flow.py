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

from scripts.core.data_container.container import DataContainer
from scripts.core.pipeline.step_executor import StepExecutor

# --- Framework components setup ---
step_executor = StepExecutor()

@task
def execute_step_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    """ A reusable Prefect task to run any plugin step. """
    inputs = inputs or {}
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    return step_executor.execute_step(step_config, inputs)

# --- ETL2 Main Flow Definition ---
@flow(name="ETL2 - CSV to NGSI-v2 (Final Design)", log_prints=True)
def etl2_main_flow():
    """
    Defines and runs ETL2 with the final, robust file-passing architecture.
    """

    # --- 1. Define all file paths for the pipeline ---
    working_dir = project_root / "ETL2/01_data/working"

    source_file = project_root / "ETL2/01_data/input/measurements_ok.csv"
    # source_file = project_root / "ETL2/01_data/input/measurements_ng.csv"
    staged_file = working_dir / "01_staged.parquet"
    pivoted_file = working_dir / "02_pivoted.parquet"
    validated_file = working_dir / "03_validated.parquet"
    structured_file = working_dir / "04_structured.parquet"
    ngsi_jsonl_file = working_dir / "05_ngsi.jsonl"
    validated_ngsi_file = working_dir / "06_validated_ngsi.jsonl"
    final_output_file = project_root / "ETL2/01_data/output/ngsi_entities.jsonl"

    # --- 2. Define the pipeline DAG by chaining tasks ---

    # Step 1: Extract
    extract_result = execute_step_task.submit(
        step_name="extract_measurements_csv",
        plugin_name="from_local_file",
        params={"source_path": source_file, "output_path": staged_file}
    )

    # Step 2: Pivot
    pivot_result = execute_step_task.submit(
        step_name="pivot_raw_data",
        plugin_name="with_duckdb",
        params={
            "output_path": pivoted_file,
            "query_file": project_root / "ETL2/05_queries/00_pivot_data.sql",
            "table_name": "measurements",
        },
        inputs={"input_data": extract_result}
    )

    # Step 3: Validate
    validate_result = execute_step_task.submit(
        step_name="validate_pivoted_data",
        plugin_name="business_rules",
        params={
            "output_path": validated_file,
            "rules": [
                {"name": "Temp out of range", "expression": "temperature < -50 or temperature > 100"},
                {"name": "Humidity out of range", "expression": "humidity < 0 or humidity > 100"},
                {"name": "Timestamp missing", "expression": "timestamp.isnull()"},
            ]
        },
        inputs={"input_data": pivot_result}
    )

    # Step 4: Structure for NGSI
    structure_result = execute_step_task.submit(
        step_name="structure_data_for_ngsi",
        plugin_name="with_duckdb",
        params={
            "output_path": structured_file,
            "query_file": project_root / "ETL2/05_queries/02_structure_to_ngsi.sql",
            "table_name": "measurements",
        },
        inputs={"input_data": validate_result}
    )

    # Step 5: Transform to NGSI JSON Lines
    to_ngsi_result = execute_step_task.submit(
        step_name="transform_to_ngsi_entities",
        plugin_name="to_ngsi",
        params={
            "output_path": ngsi_jsonl_file,
            "template_path": project_root / "ETL2/04_templates/to_ngsiv2.json.j2",
            "entity_type": "Measurement",
            "id_prefix": "urn:ngsi-ld:Measurement:",
            "id_column": "sensor_id",
        },
        inputs={"input_data": structure_result}
    )

    # Step 6: Validate NGSI JSON Lines
    validate_ngsi_result = execute_step_task.submit(
        step_name="validate_ngsi_output",
        plugin_name="ngsi_validator",
        params={
            "output_path": validated_ngsi_file,
            "ngsi_version": "v2",
        },
        inputs={"input_data": to_ngsi_result}
    )

    # Step 7: Load to final destination
    execute_step_task.submit(
        step_name="load_to_final_jsonl",
        plugin_name="to_local_file",
        params={
            "output_path": final_output_file,
        },
        inputs={"input_data": validate_ngsi_result}
    )

if __name__ == "__main__":
    etl2_main_flow()