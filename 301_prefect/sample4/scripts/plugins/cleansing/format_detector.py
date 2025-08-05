# scripts/plugins/cleansing/format_detector.py

import json
import csv
import xml.etree.ElementTree as ET
import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class FormatDetector(BaseCleanser):
    """
    Detects the format of files based on their content, not their extension.

    This cleanser inspects the beginning of each file to guess its format
    (e.g., CSV, JSON, ZIP). The detected format is then stored in the
    DataContainer's metadata for use in subsequent steps.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the format detector.
        Currently, it does not require any specific parameters.
        """
        super().__init__(params)
        self.read_chunk_size = 4096  # Read first 4KB for detection

    def _detect_format(self, file_path: Path) -> SupportedFormats:
        """
        Tries to determine the file format by inspecting its content.
        """
        try:
            # --- Binary format checks (quickest) ---
            if zipfile.is_zipfile(file_path):
                return SupportedFormats.ZIP
            if tarfile.is_tarfile(file_path):
                return SupportedFormats.TAR
            
            # Note: For Parquet, you would typically use a library like pyarrow
            # try:
            #     import pyarrow.parquet as pq
            #     if pq.is_parquet(file_path):
            #         return SupportedFormats.PARQUET
            # except (ImportError, TypeError):
            #     pass

            # --- Text-based format checks ---
            with open(file_path, 'rb') as f:
                chunk = f.read(self.read_chunk_size)
            
            # Try to decode as text. If it fails, it's likely binary.
            try:
                text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError:
                return SupportedFormats.BINARY

            # JSON check
            if text_chunk.startswith(('{', '[')):
                try:
                    json.loads(text_chunk)
                    return SupportedFormats.JSON
                except json.JSONDecodeError:
                    pass

            # XML check
            if text_chunk.startswith('<'):
                try:
                    ET.fromstring(text_chunk)
                    return SupportedFormats.XML
                except ET.ParseError:
                    pass

            # CSV check using the sniffer
            try:
                # The sniffer works best with a sample of the text
                csv.Sniffer().sniff(text_chunk, delimiters=',;\t|')
                return SupportedFormats.CSV
            except csv.Error:
                pass
            
            # If all checks fail, classify as plain text or unknown
            return SupportedFormats.TEXT

        except Exception as e:
            print(f"Could not access or read file {file_path} for format detection: {e}")
            return SupportedFormats.UNKNOWN

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Detects the format for each file in the DataContainer and updates metadata.

        Args:
            data (DataContainer): The input container with file paths.

        Returns:
            DataContainer: The same container with updated metadata. This plugin
                           does not create a new container.
        """
        if not data.file_paths:
            print("Warning: FormatDetector received no file paths to process.")
            return data
        
        detected_formats = data.metadata.get('detected_formats', {})

        for file_path in data.file_paths:
            if not file_path.is_file():
                print(f"Warning: Path '{file_path}' is not a file. Skipping format detection.")
                continue

            print(f"Detecting format for: {file_path.name}")
            detected_format = self._detect_format(file_path)
            print(f" -> Detected format: {detected_format.value}")
            detected_formats[str(file_path)] = detected_format.value

        # Update the metadata of the existing container
        data.metadata['detected_formats'] = detected_formats
        
        # This plugin modifies metadata but doesn't transform the data itself,
        # so it returns the original container instance.
        return data