# scripts/plugins/cleansing/archive_extractor.py

import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class ArchiveExtractor(BaseCleanser):
    """
    Extracts files from an archive (e.g., .zip, .tar.gz) into a directory.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.destination_dir = Path(self.params.get("destination_dir"))
        if not self.destination_dir:
            raise ValueError("ArchiveExtractor requires a 'destination_dir' parameter.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("ArchiveExtractor requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: ArchiveExtractor received a DataContainer with no file paths to process.")
            return data

        self.destination_dir.mkdir(parents=True, exist_ok=True)

        extracted_files = []
        non_archive_files = []

        for archive_path in data.file_paths:
            print(f"Processing path: {archive_path}")
            is_archive = False

            if zipfile.is_zipfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a ZIP archive. Extracting...")
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(self.destination_dir)
                    for name in zip_ref.namelist():
                        extracted_files.append(self.destination_dir / name)
                print(f"Extracted {len(extracted_files)} files to '{self.destination_dir}'.")

            elif tarfile.is_tarfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a Tar archive. Extracting...")
                with tarfile.open(archive_path, 'r:*') as tar_ref:
                    tar_ref.extractall(self.destination_dir)
                    for member in tar_ref.getmembers():
                        if member.isfile():
                            extracted_files.append(self.destination_dir / member.name)
                print(f"Extracted {len(extracted_files)} files to '{self.destination_dir}'.")

            if not is_archive:
                print(f"'{archive_path.name}' is not a recognized archive. Passing through.")
                non_archive_files.append(archive_path)

        output_container = DataContainer()
        output_container.file_paths = extracted_files + non_archive_files
        output_container.metadata = data.metadata.copy()
        output_container.metadata['archive_extracted'] = True

        return output_container