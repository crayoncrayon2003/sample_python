# scripts/plugins/extractors/from_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class FtpExtractor(BaseExtractor):
    """
    Extracts a file from an FTP server.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.host = self.params.get("host")
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.remote_path = self.params.get("remote_path")
        self.local_dir = Path(self.params.get("local_dir", "./temp/ftp/"))
        if not all([self.host, self.remote_path, self.local_dir]):
            raise ValueError("FtpExtractor requires 'host', 'remote_path', and 'local_dir'.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        self.local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(self.remote_path).name
        local_filepath = self.local_dir / remote_filename
        print(f"Connecting to FTP server at {self.host}...")
        try:
            with ftplib.FTP(self.host) as ftp:
                ftp.login(user=self.user, passwd=self.password)
                print(f"Logged in as '{self.user or 'anonymous'}'.")
                print(f"Downloading '{self.remote_path}' to '{local_filepath}'...")
                with open(local_filepath, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {self.remote_path}', local_file.write)
                print("File downloaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            if local_filepath.exists(): local_filepath.unlink()
            raise

        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata.update({
            'source_type': 'ftp', 'ftp_host': self.host,
            'remote_path': self.remote_path, 'local_path': str(local_filepath)
        })
        return container