# scripts/plugins/cleansing/format_detector.py

import json
import csv
import xml.etree.ElementTree as ET
import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class FormatDetector(BaseCleanser):
    """
    Detects the format of files based on their content, not their extension.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.read_chunk_size = 4096

    def _detect_format(self, file_path: Path) -> SupportedFormats:
        try:
            if zipfile.is_zipfile(file_path): return SupportedFormats.ZIP
            if tarfile.is_tarfile(file_path): return SupportedFormats.TAR

            with open(file_path, 'rb') as f:
                chunk = f.read(self.read_chunk_size)

            try:
                text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError:
                return SupportedFormats.BINARY

            if text_chunk.startswith(('{', '[')):
                try:
                    json.loads(text_chunk); return SupportedFormats.JSON
                except json.JSONDecodeError: pass

            if text_chunk.startswith('<'):
                try:
                    ET.fromstring(text_chunk); return SupportedFormats.XML
                except ET.ParseError: pass

            try:
                csv.Sniffer().sniff(text_chunk, delimiters=',;\t|'); return SupportedFormats.CSV
            except csv.Error: pass

            return SupportedFormats.TEXT

        except Exception as e:
            print(f"Could not access or read file {file_path} for format detection: {e}")
            return SupportedFormats.UNKNOWN

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("FormatDetector requires a single input named 'input_data'.")
        data = inputs['input_data']

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

        data.metadata['detected_formats'] = detected_formats
        return data