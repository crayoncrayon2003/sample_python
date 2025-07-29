# scripts/tasks/load/to_local_file.py
import shutil
from pathlib import Path
from typing import Union

from prefect import task


@task
def load_to_local_file(input_path: Union[str, Path], output_path: Path) -> str:
    """
    Saves a processed data file to its final local destination.

    This task typically runs at the end of an ETL pipeline, moving an
    intermediate file from a working directory to a final output directory.
    It ensures the destination directory exists before copying.

    Args:
        input_path: The path to the source file (e.g., in a working directory).
                    Can be a string or a Path object.
        output_path: The final destination path for the file.

    Returns:
        The final destination path as a string.
    """
    print(f"Load task: Copying {input_path} to {output_path}...")

    # Ensure the destination directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Copy the file to the final destination
    shutil.copy(str(input_path), str(output_path))

    print(f"Load successful: File saved to {output_path}")
    return str(output_path)