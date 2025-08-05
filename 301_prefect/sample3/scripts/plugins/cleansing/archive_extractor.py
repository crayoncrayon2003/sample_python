# scripts/plugins/cleansing/archive_extractor.py

import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class ArchiveExtractor(BaseCleanser):
    """
    Extracts files from an archive (e.g., .zip, .tar.gz) into a directory.

    This cleanser checks the input DataContainer for file paths pointing to
    supported archive types. If found, it extracts their contents into a
    specified destination directory. The paths of the extracted files are
    then passed on in a new DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with parameters for archive handling.

        Expected params:
            - destination_dir (str): The local directory where a
              rchive contents will be extracted.
        """
        super().__init__(params)
        self.destination_dir = Path(self.params.get("destination_dir"))

        if not self.destination_dir:
            raise ValueError("ArchiveExtractor requires a 'destination_dir' parameter.")

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Iterates through files in the input DataContainer and extracts any archives.

        Args:
            data (DataContainer): The input container, expected to have one or
                                  more file paths.

        Returns:
            DataContainer: A new container with paths to the extracted files.
                           Non-archive files from the input are passed through.
        """
        if not data.file_paths:
            print("Warning: ArchiveExtractor received a DataContainer with no file paths to process.")
            return data

        self.destination_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        non_archive_files = []

        for archive_path in data.file_paths:
            print(f"Processing path: {archive_path}")
            is_archive = False
            
            # --- ZIP file handling ---
            if zipfile.is_zipfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a ZIP archive. Extracting...")
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(self.destination_dir)
                    # Add all extracted file paths to the list
                    for name in zip_ref.namelist():
                        extracted_files.append(self.destination_dir / name)
                print(f"Extracted {len(extracted_files)} files to '{self.destination_dir}'.")
            
            # --- Tar file handling (including .gz, .bz2) ---
            elif tarfile.is_tarfile(archive_path):
                is_archive = True
                print(f"'{archive_path.name}' is a Tar archive. Extracting...")
                # 'r:*' allows tarfile to automatically handle compression (gz, bz2)
                with tarfile.open(archive_path, 'r:*') as tar_ref:
                    tar_ref.extractall(self.destination_dir)
                    for member in tar_ref.getmembers():
                        if member.isfile():
                            extracted_files.append(self.destination_dir / member.name)
                print(f"Extracted {len(extracted_files)} files to '{self.destination_dir}'.")
            
            if not is_archive:
                print(f"'{archive_path.name}' is not a recognized archive. Passing through.")
                non_archive_files.append(archive_path)

        # Create a new container for the output
        output_container = DataContainer()
        # Add the newly extracted files and any files that were not archives
        output_container.file_paths = extracted_files + non_archive_files
        
        # Copy metadata from the original container to the new one
        output_container.metadata = data.metadata.copy()
        output_container.metadata['archive_extracted'] = True
        
        return output_container