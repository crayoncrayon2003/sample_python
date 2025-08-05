# scripts/plugins/extractors/from_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class ScpExtractor(BaseExtractor):
    """
    Extracts a file from a remote server using SCP (Secure Copy Protocol).

    This extractor establishes an SSH connection to a remote server, downloads
    a specified file to a local directory via SCP, and then passes the path
    to the downloaded file to the next step in the pipeline.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with SSH/SCP connection details.

        Expected params:
            - host (str): The hostname or IP address of the remote server.
            - port (int, optional): The SSH port. Defaults to 22.
            - user (str): The username for SSH authentication.
            - password (str, optional): The password for authentication. If not
              provided, `key_filepath` must be specified.
            - key_filepath (str, optional): The path to the private SSH key for
              key-based authentication.
            - remote_path (str): The full path to the file on the remote server.
            - local_dir (str): The local directory to download the file into.
        """
        super().__init__(params)
        self.host = self.params.get("host")
        self.port = self.params.get("port", 22)
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.key_filepath = self.params.get("key_filepath")
        self.remote_path = self.params.get("remote_path")
        self.local_dir = Path(self.params.get("local_dir", "./temp/scp/"))

        if not self.host or not self.user or not self.remote_path:
            raise ValueError("ScpExtractor requires 'host', 'user', and 'remote_path' parameters.")
        if not self.password and not self.key_filepath:
            raise ValueError("ScpExtractor requires either a 'password' or a 'key_filepath' for authentication.")

    def execute(self) -> DataContainer:
        """
        Connects to the remote server via SSH and downloads the file using SCP.

        Returns:
            DataContainer: A container with the path to the downloaded local file.
        
        Raises:
            Exception: If there is an error during the SSH connection or SCP transfer.
        """
        self.local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(self.remote_path).name
        local_filepath = self.local_dir / remote_filename

        ssh_client = None
        try:
            # Initialize SSH client
            ssh_client = paramiko.SSHClient()
            # Automatically add host keys (less secure, consider known_hosts for production)
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            print(f"Connecting to {self.host} as user '{self.user}'...")
            ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                key_filename=self.key_filepath,
                timeout=30
            )
            print("SSH connection established.")

            # Open SCP client session
            with ssh_client.open_sftp() as sftp:
                print(f"Downloading '{self.remote_path}' to '{local_filepath}' via SCP...")
                sftp.get(self.remote_path, str(local_filepath))
                print("File downloaded successfully.")

        except Exception as e:
            # Catch a broad range of paramiko and socket errors
            print(f"SCP operation failed: {e}")
            # Clean up partially downloaded file if it exists
            if local_filepath.exists():
                local_filepath.unlink()
            raise
        finally:
            # Ensure the SSH client is closed
            if ssh_client:
                ssh_client.close()
                print("SSH connection closed.")

        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata['source_type'] = 'scp'
        container.metadata['remote_host'] = self.host
        container.metadata['remote_path'] = self.remote_path
        container.metadata['local_path'] = str(local_filepath)

        return container