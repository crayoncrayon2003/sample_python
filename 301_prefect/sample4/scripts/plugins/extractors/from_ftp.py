# scripts/plugins/extractors/from_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class FtpExtractor(BaseExtractor):
    """
    Extracts a file from an FTP server.

    This extractor connects to an FTP server, downloads a specified file to a
    local temporary directory, and then passes the path to that file to the
    next step in the pipeline within a DataContainer.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with FTP connection and file details.

        Expected params:
            - host (str): The hostname or IP address of the FTP server.
            - user (str, optional): The username for authentication.
            - password (str, optional): The password for authentication.
            - remote_path (str): The full path to the file on the FTP server.
            - local_dir (str): The local directory to download the file into.
        """
        super().__init__(params)
        self.host = self.params.get("host")
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.remote_path = self.params.get("remote_path")
        self.local_dir = Path(self.params.get("local_dir", "./temp/ftp/"))

        if not all([self.host, self.remote_path, self.local_dir]):
            raise ValueError("FtpExtractor requires 'host', 'remote_path', and 'local_dir' parameters.")

    def execute(self) -> DataContainer:
        """
        Connects to the FTP server and downloads the file.

        Returns:
            DataContainer: A container without a DataFrame, but with the
                           path to the downloaded local file.
        
        Raises:
            ftplib.all_errors: If there is an error during the FTP operations.
        """
        self.local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(self.remote_path).name
        local_filepath = self.local_dir / remote_filename

        print(f"Connecting to FTP server at {self.host}...")
        try:
            # Use a 'with' statement for automatic connection closing
            with ftplib.FTP(self.host) as ftp:
                ftp.login(user=self.user, passwd=self.password)
                print(f"Logged in as '{self.user or 'anonymous'}'.")
                
                print(f"Downloading '{self.remote_path}' to '{local_filepath}'...")
                with open(local_filepath, 'wb') as local_file:
                    # The callback function can be used for progress indication
                    # ftp.retrbinary(f'RETR {self.remote_path}', local_file.write)
                    
                    # For simplicity, we use a basic download command
                    ftp.retrbinary(f'RETR {self.remote_path}', local_file.write)

                print("File downloaded successfully.")

        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            # Clean up partially downloaded file if it exists
            if local_filepath.exists():
                local_filepath.unlink()
            raise

        # This extractor's job is just to download the file.
        # It creates a DataContainer that points to the local file path.
        # The actual parsing of the file is delegated to a subsequent step.
        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata['source_type'] = 'ftp'
        container.metadata['ftp_host'] = self.host
        container.metadata['remote_path'] = self.remote_path
        container.metadata['local_path'] = str(local_filepath)

        return container