# ETL2/00_flows/etl_flow.py

import os
import sys
from pathlib import Path

# --- Add project root to Python path ---
try:
    project_root = Path(__file__).resolve().parents[2]
    if "scripts" not in os.listdir(project_root):
        raise FileNotFoundError
except (IndexError, FileNotFoundError):
    project_root = Path(os.getcwd())

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# -----------------------------------------

from prefect import flow

# Import the main builder and the task runner from our framework
from scripts.core.pipeline.pipeline_builder import PipelineBuilder
from scripts.core.orchestrator.task_runner import run_pipeline_as_task

@flow(name="ETL2 - CSV to NGSI-v2 (Code-Based)", log_prints=True)
def etl2_main_flow():
    """
    Defines and runs the ETL2 pipeline using the code-based PipelineBuilder.
    This flow transforms measurements data from a CSV file into NGSI-v2 entities.
    """

    # --- 1. Initialize the Pipeline Builder ---
    etl2_pipeline = PipelineBuilder(name="ETL2 - CSV to NGSI-v2")

    # --- 2. Build the pipeline by adding steps sequentially ---
    (etl2_pipeline
        .add_step(
            plugin="from_local_file",
            name="Extract Measurements CSV",
            params={
                "path": project_root / "ETL2/01_data/input/measurements_ok.csv"
                # "path": project_root / "ETL2/01_data/input/measurements_ng.csv"
            }
        )
        .add_step(
            plugin="with_duckdb",
            name="Pivot Raw Data",
            params={
                "query_file": project_root / "ETL2/05_queries/00_pivot_data.sql",
                "table_name": "measurements",
            }
        )
        .add_step(
            plugin="business_rules",
            name="Validate Pivoted Data",
            params={
                "rules": [
                    {"name": "Temperature out of plausible range", "expression": "temperature < -50 or temperature > 100"},
                    {"name": "Humidity out of plausible range", "expression": "humidity < 0 or humidity > 100"},
                    {"name": "Timestamp is missing", "expression": "timestamp.isnull()"},
                ]
            }
        )
        .add_step(
            plugin="with_duckdb",
            name="Structure Data for NGSI",
            params={
                "query_file": project_root / "ETL2/05_queries/02_structure_to_ngsi.sql",
                "table_name": "measurements",
            }
        )
        .add_step(
            plugin="to_ngsi",
            name="Transform to NGSI-v2 Entities",
            params={
                "template_path": project_root / "ETL2/04_templates/to_ngsiv2.json.j2",
                "entity_type": "Measurement",
                "id_prefix": "urn:ngsi-ld:Measurement:",
                "id_column": "sensor_id",
                "output_column_name": "ngsi_entity_json",
            }
        )
        .add_step(
            plugin="ngsi_validator",
            name="Validate NGSI-v2 Output",
            params={
                "target_column": "ngsi_entity_json",
                "ngsi_version": "v2",
            }
        )
        .add_step(
            plugin="to_local_file",
            name="Load to JSONL File",
            params={
                "output_path": project_root / "ETL2/01_data/output/ngsi_entities.json",
                "format": "json",
                "pandas_options": {
                    "orient": "records",
                    "lines": True,
                },
            }
        )
    )

    # --- 3. Run the entire configured pipeline as a single Prefect task ---
    run_pipeline_as_task(pipeline=etl2_pipeline)


if __name__ == "__main__":
    etl2_main_flow()