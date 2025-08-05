# scripts/plugins/cleansing/encoding_converter.py

from pathlib import Path
from typing import Dict, Any, Optional
import charset_normalizer

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class EncodingConverter(BaseCleanser):
    """
    Converts the character encoding of text-based files to a target encoding.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.target_encoding = self.params.get("target_encoding", "utf-8")
        self.source_encoding = self.params.get("source_encoding")
        self.output_dir = Path(self.params.get("output_dir"))
        self.suffix = self.params.get("filename_suffix", "_utf8")

        if not self.output_dir:
            raise ValueError("EncodingConverter requires an 'output_dir' parameter.")

    def _detect_encoding(self, file_path: Path) -> str:
        with open(file_path, 'rb') as f:
            raw_data = f.read()
        try:
            result = charset_normalizer.from_bytes(raw_data).best()
            if result:
                detected_encoding = result.encoding
                print(f"Detected encoding for '{file_path.name}': {detected_encoding}")
                return detected_encoding
        except Exception as e:
            print(f"Could not detect encoding for '{file_path.name}': {e}")

        print("Warning: Encoding detection failed. Falling back to default.")
        return 'latin-1'

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> DataContainer:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("EncodingConverter requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: EncodingConverter received no file paths to process.")
            return data

        self.output_dir.mkdir(parents=True, exist_ok=True)
        converted_files = []

        for file_path in data.file_paths:
            print(f"Converting encoding for: {file_path}")

            source_enc = self.source_encoding or self._detect_encoding(file_path)

            output_filename = f"{file_path.stem}{self.suffix}{file_path.suffix}"
            output_path = self.output_dir / output_filename

            try:
                with open(file_path, 'r', encoding=source_enc, errors='replace') as infile:
                    content = infile.read()

                with open(output_path, 'w', encoding=self.target_encoding) as outfile:
                    outfile.write(content)

                converted_files.append(output_path)
                print(f"Successfully converted and saved to '{output_path}'.")

            except (UnicodeDecodeError, UnicodeEncodeError, LookupError) as e:
                print(f"ERROR converting '{file_path.name}': {e}. Skipping file.")
                continue
            except Exception as e:
                print(f"An unexpected error occurred with '{file_path.name}': {e}")
                raise

        output_container = DataContainer()
        output_container.file_paths = converted_files
        output_container.metadata = data.metadata.copy()
        output_container.metadata['encoding_converted_to'] = self.target_encoding

        return output_container