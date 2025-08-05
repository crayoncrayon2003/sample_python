# scripts/plugins/loaders/to_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, List

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class ScpLoader(BaseLoader):
    """
    Loads (uploads) local files to a remote server using SCP.

    This loader establishes an SSH connection and uploads each file listed
    in the DataContainer's file_paths to a specified remote directory.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with SSH/SCP connection details.

        Expected params:
            - host (str): The hostname or IP address of the remote server.
            - port (int, optional): The SSH port. Defaults to 22.
            - user (str): The username for SSH authentication.
            - password (str, optional): The password. If not provided,
              `key_filepath` must be specified.
            - key_filepath (str, optional): Path to the private SSH key.
            - remote_dir (str): The target directory on the remote server
              to upload files into.
        """
        super().__init__(params)
        self.host = self.params.get("host")
        self.port = self.params.get("port", 22)
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.key_filepath = self.params.get("key_filepath")
        self.remote_dir = self.params.get("remote_dir")

        if not self.host or not self.user or not self.remote_dir:
            raise ValueError("ScpLoader requires 'host', 'user', and 'remote_dir' parameters.")
        if not self.password and not self.key_filepath:
            raise ValueError("ScpLoader requires either a 'password' or a 'key_filepath'.")

    def execute(self, data: DataContainer) -> None:
        """
        Connects to the remote server and uploads files via SCP.

        Args:
            data (DataContainer): The container with local file paths to upload.
        """
        if not data.file_paths:
            print("Warning: ScpLoader received no file paths to upload. Skipping.")
            return

        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            print(f"Connecting to {self.host} as user '{self.user}' for SCP upload...")
            ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                key_filename=self.key_filepath,
                timeout=30
            )
            print("SSH connection established.")

            # Open an SFTP client session to perform file transfers
            with ssh_client.open_sftp() as sftp:
                print(f"Uploading {len(data.file_paths)} files to remote directory '{self.remote_dir}'...")
                
                # Check if remote directory exists, create if not
                try:
                    sftp.stat(self.remote_dir)
                except FileNotFoundError:
                    print(f"Remote directory '{self.remote_dir}' not found. Creating it.")
                    sftp.mkdir(self.remote_dir)

                for file_path in data.file_paths:
                    if not file_path.is_file():
                        print(f"Warning: Path '{file_path}' is not a file. Skipping upload.")
                        continue
                    
                    remote_path = f"{self.remote_dir}/{file_path.name}"
                    print(f"  Uploading '{file_path.name}' to '{remote_path}'...")
                    sftp.put(str(file_path), remote_path)
            
            print("All files uploaded successfully.")

        except Exception as e:
            print(f"SCP operation failed: {e}")
            raise
        finally:
            if ssh_client:
                ssh_client.close()
                print("SSH connection closed.")