# scripts/plugins/cleansing/encoding_converter.py

from pathlib import Path
from typing import Dict, Any
import charset_normalizer

from .base import BaseCleanser
from scripts.core.data_container.container import DataContainer

class EncodingConverter(BaseCleanser):
    """
    Converts the character encoding of text-based files to a target encoding.

    This cleanser reads files specified in the input DataContainer, detects
    their original encoding (or uses a specified one), and rewrites them
    in a target encoding (typically UTF-8). The paths to the newly
    created files are passed on in the output DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the converter with encoding parameters.

        Expected params:
            - target_encoding (str): The encoding to convert files to.
              Defaults to 'utf-8'.
            - source_encoding (str, optional): The encoding of the source files.
              If not provided, the encoding will be automatically detected.
            - output_dir (str): The directory to save the converted files in.
            - filename_suffix (str, optional): A suffix to add to the converted
              filenames. Defaults to '_utf8'.
        """
        super().__init__(params)
        self.target_encoding = self.params.get("target_encoding", "utf-8")
        self.source_encoding = self.params.get("source_encoding")
        self.output_dir = Path(self.params.get("output_dir"))
        self.suffix = self.params.get("filename_suffix", "_utf8")
        
        if not self.output_dir:
            raise ValueError("EncodingConverter requires an 'output_dir' parameter.")

    def _detect_encoding(self, file_path: Path) -> str:
        """
        Detects the encoding of a file using charset_normalizer.
        """
        with open(file_path, 'rb') as f:
            raw_data = f.read()
        
        try:
            # charset-normalizer returns the best match
            result = charset_normalizer.from_bytes(raw_data).best()
            if result:
                detected_encoding = result.encoding
                print(f"Detected encoding for '{file_path.name}': {detected_encoding}")
                return detected_encoding
        except Exception as e:
            print(f"Could not detect encoding for '{file_path.name}': {e}")
        
        print("Warning: Encoding detection failed. Falling back to default.")
        return 'latin-1' # A safe fallback that rarely fails

    def execute(self, data: DataContainer) -> DataContainer:
        """
        Reads input files, converts their encoding, and saves them as new files.

        Args:
            data (DataContainer): The container with paths to the files to convert.

        Returns:
            DataContainer: A container with paths to the newly encoded files.
        """
        if not data.file_paths:
            print("Warning: EncodingConverter received no file paths to process.")
            return data

        self.output_dir.mkdir(parents=True, exist_ok=True)
        converted_files = []

        for file_path in data.file_paths:
            print(f"Converting encoding for: {file_path}")

            # Determine the source encoding
            source_enc = self.source_encoding
            if not source_enc:
                source_enc = self._detect_encoding(file_path)

            # Define the output path
            output_filename = f"{file_path.stem}{self.suffix}{file_path.suffix}"
            output_path = self.output_dir / output_filename

            try:
                # Read with source encoding, write with target encoding
                with open(file_path, 'r', encoding=source_enc, errors='replace') as infile:
                    content = infile.read()
                
                with open(output_path, 'w', encoding=self.target_encoding) as outfile:
                    outfile.write(content)
                
                converted_files.append(output_path)
                print(f"Successfully converted and saved to '{output_path}'.")

            except (UnicodeDecodeError, UnicodeEncodeError, LookupError) as e:
                print(f"ERROR converting '{file_path.name}': {e}. Skipping file.")
                continue # Skip to the next file on error
            except Exception as e:
                print(f"An unexpected error occurred with '{file_path.name}': {e}")
                raise

        output_container = DataContainer()
        output_container.file_paths = converted_files
        output_container.metadata = data.metadata.copy()
        output_container.metadata['encoding_converted_to'] = self.target_encoding

        return output_container