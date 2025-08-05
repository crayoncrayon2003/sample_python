# ETL1/00_flows/etl_flow.py

import os
import sys
from pathlib import Path

# --- Add project root to Python path ---
try:
    # このファイルが `.../ETL1/00_flows/etl_flow.py` にあることを想定
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

@flow(name="ETL1 - CSV to Parquet (Code-Based)", log_prints=True)
def etl1_main_flow():
    """
    Defines and runs the ETL1 pipeline using the code-based PipelineBuilder.
    This flow extracts data from a CSV, validates it, transforms it,
    and loads it into a Parquet file.
    """

    # --- 1. Initialize the Pipeline Builder ---
    etl1_pipeline = PipelineBuilder(name="ETL1 - CSV to Parquet")

    # --- 2. Build the pipeline by adding steps sequentially ---
    # The parameters for each step, previously in config.yml, are now
    # passed as a Python dictionary.
    (etl1_pipeline
        .add_step(
            plugin="from_local_file",
            name="Extract Source CSV",
            params={
                "path": project_root / "ETL1/01_data/input/source_data.csv"
            }
        )
        .add_step(
            plugin="data_quality",
            name="Validate Raw Data",
            params={
                "rules": [
                    {"column": "product_id", "type": "not_null"},
                    {"column": "product_id", "type": "is_unique"},
                    {"column": "price", "type": "in_range", "min": 0},
                    {"column": "quantity", "type": "in_range", "min": 0},
                ]
            }
        )
        .add_step(
            plugin="with_duckdb",
            name="Add Timestamp with DuckDB",
            params={
                "query_file": project_root / "ETL1/05_queries/add_timestamp.sql",
                "table_name": "sales_data",
            }
        )
        .add_step(
            plugin="data_quality",
            name="Validate Transformed Data",
            params={
                "rules": [
                    {"column": "product_id", "type": "not_null"},
                    {"column": "processing_timestamp", "type": "not_null"},
                ]
            }
        )
        .add_step(
            plugin="to_local_file",
            name="Load to Parquet File",
            params={
                "output_path": project_root / "ETL1/01_data/output/processed_sales.parquet",
                "format": "parquet",
            }
        )
    )

    # --- 3. Run the entire configured pipeline as a single Prefect task ---
    run_pipeline_as_task(pipeline=etl1_pipeline)


if __name__ == "__main__":
    etl1_main_flow()