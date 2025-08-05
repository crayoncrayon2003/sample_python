# scripts/plugins/extractors/from_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer

class ScpExtractor(BaseExtractor):
    """
    Extracts a file from a remote server using SCP.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.host = self.params.get("host")
        self.port = self.params.get("port", 22)
        self.user = self.params.get("user")
        self.password = self.params.get("password")
        self.key_filepath = self.params.get("key_filepath")
        self.remote_path = self.params.get("remote_path")
        self.local_dir = Path(self.params.get("local_dir", "./temp/scp/"))
        if not self.host or not self.user or not self.remote_path:
            raise ValueError("ScpExtractor requires 'host', 'user', and 'remote_path'.")
        if not self.password and not self.key_filepath:
            raise ValueError("ScpExtractor requires either 'password' or 'key_filepath'.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        self.local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(self.remote_path).name
        local_filepath = self.local_dir / remote_filename
        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print(f"Connecting to {self.host} as user '{self.user}'...")
            ssh_client.connect(
                hostname=self.host, port=self.port, username=self.user,
                password=self.password, key_filename=self.key_filepath, timeout=30
            )
            print("SSH connection established.")
            with ssh_client.open_sftp() as sftp:
                print(f"Downloading '{self.remote_path}' to '{local_filepath}' via SCP...")
                sftp.get(self.remote_path, str(local_filepath))
                print("File downloaded successfully.")
        except Exception as e:
            print(f"SCP operation failed: {e}")
            if local_filepath.exists(): local_filepath.unlink()
            raise
        finally:
            if ssh_client: ssh_client.close(); print("SSH connection closed.")

        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata.update({
            'source_type': 'scp', 'remote_host': self.host,
            'remote_path': self.remote_path, 'local_path': str(local_filepath)
        })
        return container