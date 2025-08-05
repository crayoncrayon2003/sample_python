# ETL2/00_flows/etl_flow.py

import os
import sys
from pathlib import Path

# --- Add project root to Python path ---
try:
    from scripts.utils.file_utils import get_project_root
    project_root = get_project_root()
except ImportError:
    project_root = Path(__file__).resolve().parents[2]

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# -----------------------------------------

from scripts.core.orchestrator.flow_executor import FlowExecutor

def run_etl2_flow():
    """
    Main function to execute the ETL2 pipeline.
    """
    print("--- Starting ETL2: CSV to NGSI-v2 JSON ---")

    # Path to the configuration file for this specific ETL pipeline.
    config_file = project_root / "ETL2" / "config.yml"

    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        sys.exit(1)

    try:
        runner = FlowExecutor(config_path=str(config_file))
        runner.run()
        
        print("--- ETL2 Flow finished successfully. ---")

    except Exception as e:
        print(f"--- ETL2 Flow failed: {e} ---")
        raise

if __name__ == "__main__":
    run_etl2_flow()