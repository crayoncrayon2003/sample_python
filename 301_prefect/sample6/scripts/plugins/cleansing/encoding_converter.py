# scripts/plugins/cleansing/encoding_converter.py

from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import charset_normalizer

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class EncodingConverter:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "encoding_converter"

    def _detect_encoding(self, file_path: Path) -> str:
        with open(file_path, 'rb') as f: raw_data = f.read()
        try:
            result = charset_normalizer.from_bytes(raw_data).best()
            if result:
                print(f"Detected encoding for '{file_path.name}': {result.encoding}")
                return result.encoding
        except Exception as e:
            print(f"Could not detect encoding for '{file_path.name}': {e}")
        print("Warning: Encoding detection failed. Falling back to default.")
        return 'latin-1'

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        target_encoding = params.get("target_encoding", "utf-8")
        source_encoding = params.get("source_encoding")
        output_dir = Path(params.get("output_dir"))
        suffix = params.get("filename_suffix", "_utf8")
        if not output_dir: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_dir'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: EncodingConverter received no file paths.")
            return data

        output_dir.mkdir(parents=True, exist_ok=True)
        converted_files = []
        for file_path in data.file_paths:
            print(f"Converting encoding for: {file_path}")
            source_enc = source_encoding or self._detect_encoding(file_path)
            output_filename = f"{file_path.stem}{suffix}{file_path.suffix}"
            output_path = output_dir / output_filename
            try:
                with open(file_path, 'r', encoding=source_enc, errors='replace') as infile:
                    content = infile.read()
                with open(output_path, 'w', encoding=target_encoding) as outfile:
                    outfile.write(content)
                converted_files.append(output_path)
                print(f"Successfully converted and saved to '{output_path}'.")
            except Exception as e:
                print(f"ERROR converting '{file_path.name}': {e}. Skipping.")
                continue

        output_container = DataContainer()
        output_container.file_paths = converted_files
        output_container.metadata = data.metadata.copy()
        output_container.metadata['encoding_converted_to'] = target_encoding
        return output_container