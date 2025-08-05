# scripts/plugins/loaders/to_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, List, Optional

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class FtpLoader(BaseLoader):
    """
    Loads (uploads) local files to an FTP server.
    """

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.host = self.params.get("host")
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.remote_dir = self.params.get("remote_dir", "/")
        if not self.host:
            raise ValueError("FtpLoader requires a 'host' parameter.")

    def _upload_file(self, ftp: ftplib.FTP, local_path: Path) -> None:
        remote_filename = local_path.name
        print(f"  Uploading '{local_path.name}' to '{self.remote_dir}'...")
        with open(local_path, 'rb') as local_file:
            ftp.storbinary(f'STOR {remote_filename}', local_file)
        print(f"  Upload of '{remote_filename}' successful.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]]) -> None:
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError("FtpLoader requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: FtpLoader received no file paths to upload. Skipping.")
            return

        print(f"Connecting to FTP server at {self.host} to upload {len(data.file_paths)} files...")
        
        try:
            with ftplib.FTP(self.host, timeout=60) as ftp:
                ftp.login(user=self.user, passwd=self.password)
                print(f"Logged in as '{self.user or 'anonymous'}'.")
                
                if self.remote_dir != '/':
                    print(f"Changing remote directory to '{self.remote_dir}'...")
                    ftp.cwd(self.remote_dir)
                
                for file_path in data.file_paths:
                    if not file_path.is_file():
                        print(f"Warning: Path '{file_path}' is not a file. Skipping upload.")
                        continue
                    self._upload_file(ftp, file_path)
            print("All files uploaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            raise