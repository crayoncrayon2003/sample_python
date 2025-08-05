# scripts/plugins/cleansing/format_detector.py

import json, csv, tarfile, zipfile, xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

hookimpl = pluggy.HookimplMarker("etl_framework")

class FormatDetector:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "format_detector"

    def _detect_format(self, file_path: Path, read_chunk_size: int) -> SupportedFormats:
        try:
            if zipfile.is_zipfile(file_path): return SupportedFormats.ZIP
            if tarfile.is_tarfile(file_path): return SupportedFormats.TAR
            with open(file_path, 'rb') as f: chunk = f.read(read_chunk_size)
            try: text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError: return SupportedFormats.BINARY
            if text_chunk.startswith(('{', '[')):
                try: json.loads(text_chunk); return SupportedFormats.JSON
                except json.JSONDecodeError: pass
            if text_chunk.startswith('<'):
                try: ET.fromstring(text_chunk); return SupportedFormats.XML
                except ET.ParseError: pass
            try: csv.Sniffer().sniff(text_chunk, delimiters=',;\t|'); return SupportedFormats.CSV
            except csv.Error: pass
            return SupportedFormats.TEXT
        except Exception as e:
            print(f"Could not read file {file_path} for format detection: {e}")
            return SupportedFormats.UNKNOWN

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        read_chunk_size = params.get("read_chunk_size", 4096)
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: FormatDetector received no file paths.")
            return data

        detected_formats = data.metadata.get('detected_formats', {})
        for file_path in data.file_paths:
            if not file_path.is_file(): continue
            print(f"Detecting format for: {file_path.name}")
            detected_format = self._detect_format(file_path, read_chunk_size)
            print(f" -> Detected format: {detected_format.value}")
            detected_formats[str(file_path)] = detected_format.value

        data.metadata['detected_formats'] = detected_formats
        return data