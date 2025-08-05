# scripts/core/data_container/container.py

from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path

class DataContainer:
    """
    A standardized container for passing data between ETL pipeline steps.

    This class acts as a wrapper that holds not only the data itself (often
    as a pandas DataFrame) but also associated metadata, such as the original
    file path, data format, or any other contextual information that might be
    useful for subsequent processing steps.
    """

    def __init__(
        self,
        data: Optional[pd.DataFrame] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initializes the DataContainer.

        Args:
            data (Optional[pd.DataFrame]): The primary data, typically held
                in a pandas DataFrame. Defaults to None.
            metadata (Optional[Dict[str, Any]]): A dictionary for storing
                any additional information about the data. Defaults to None.
        """
        self.data: Optional[pd.DataFrame] = data
        self.metadata: Dict[str, Any] = metadata or {}

        # We can also store a list of file paths if the container represents
        # multiple files, for example, after decompressing an archive.
        self.file_paths: List[Path] = []

    def __repr__(self) -> str:
        """
        Provides a developer-friendly string representation of the container.
        """
        data_shape = self.data.shape if self.data is not None else "N/A"
        num_files = len(self.file_paths)
        metadata_keys = list(self.metadata.keys())

        return (
            f"<DataContainer | Data Shape: {data_shape} | "
            f"File Paths: {num_files} | Metadata Keys: {metadata_keys}>"
        )

    def add_file_path(self, path: str | Path):
        """
        Adds a file path associated with the data in this container.
        """
        self.file_paths.append(Path(path))

    def get_primary_file_path(self) -> Optional[Path]:
        """
        Returns the first file path in the list, if any.
        """
        return self.file_paths[0] if self.file_paths else None