# scripts/plugins/loaders/to_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, List

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class FtpLoader(BaseLoader):
    """
    Loads (uploads) local files to an FTP server.

    This loader iterates through the file paths in the input DataContainer
    and uploads each file to a specified directory on an FTP server.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with FTP connection and upload details.

        Expected params:
            - host (str): The hostname or IP address of the FTP server.
            - user (str, optional): The username for authentication.
            - password (str, optional): The password for authentication.
            - remote_dir (str, optional): The target directory on the FTP
              server to upload files into. Defaults to the root directory '/'.
        """
        super().__init__(params)
        self.host = self.params.get("host")
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.remote_dir = self.params.get("remote_dir", "/")

        if not self.host:
            raise ValueError("FtpLoader requires a 'host' parameter.")

    def _upload_file(self, ftp: ftplib.FTP, local_path: Path) -> None:
        """Uploads a single file to the current directory on the FTP server."""
        remote_filename = local_path.name
        print(f"  Uploading '{local_path.name}' to '{self.remote_dir}'...")
        
        with open(local_path, 'rb') as local_file:
            # Use 'STOR' command for uploading
            ftp.storbinary(f'STOR {remote_filename}', local_file)
        
        print(f"  Upload of '{remote_filename}' successful.")

    def execute(self, data: DataContainer) -> None:
        """
        Connects to the FTP server and uploads the files from the DataContainer.

        Args:
            data (DataContainer): The container with local file paths to upload.
        """
        if not data.file_paths:
            print("Warning: FtpLoader received no file paths to upload. Skipping.")
            return

        print(f"Connecting to FTP server at {self.host} to upload {len(data.file_paths)} files...")
        
        try:
            with ftplib.FTP(self.host, timeout=60) as ftp:
                ftp.login(user=self.user, passwd=self.password)
                print(f"Logged in as '{self.user or 'anonymous'}'.")
                
                # Change to the target remote directory
                if self.remote_dir != '/':
                    print(f"Changing remote directory to '{self.remote_dir}'...")
                    ftp.cwd(self.remote_dir)
                
                # Upload each file
                for file_path in data.file_paths:
                    if not file_path.is_file():
                        print(f"Warning: Path '{file_path}' is not a file. Skipping upload.")
                        continue
                    self._upload_file(ftp, file_path)
            
            print("All files uploaded successfully.")

        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            raise