# scripts/plugins/cleansing/archive_extractor.py

import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ArchiveExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "archive_extractor"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        destination_dir = Path(params.get("destination_dir"))
        if not destination_dir: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'destination_dir'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: ArchiveExtractor received no file paths.")
            return data

        destination_dir.mkdir(parents=True, exist_ok=True)
        extracted_files, non_archive_files = [], []

        for archive_path in data.file_paths:
            print(f"Processing path: {archive_path}")
            is_archive = False
            if zipfile.is_zipfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a ZIP. Extracting...")
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(destination_dir)
                    for name in zip_ref.namelist(): extracted_files.append(destination_dir / name)
                print(f"Extracted files to '{destination_dir}'.")
            elif tarfile.is_tarfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a Tar. Extracting...")
                with tarfile.open(archive_path, 'r:*') as tar_ref:
                    tar_ref.extractall(destination_dir)
                    for member in tar_ref.getmembers():
                        if member.isfile(): extracted_files.append(destination_dir / member.name)
                print(f"Extracted files to '{destination_dir}'.")

            if not is_archive:
                print(f"'{archive_path.name}' is not an archive. Passing through.")
                non_archive_files.append(archive_path)

        output_container = DataContainer()
        output_container.file_paths = extracted_files + non_archive_files
        output_container.metadata = data.metadata.copy()
        output_container.metadata['archive_extracted'] = True
        return output_container