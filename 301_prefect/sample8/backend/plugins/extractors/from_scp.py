# backend/plugins/extractors/from_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpExtractor:
    """
    (File-based) Extracts a file from a remote server via SCP and saves it to a local path.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_scp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "title": "SSH Host",
                    "description": "Hostname or IP address of the remote server."
                },
                "user": {
                    "type": "string",
                    "title": "SSH Username",
                    "description": "Username for SSH authentication."
                },
                "remote_path": {
                    "type": "string",
                    "title": "Remote File Path",
                    "description": "The full path to the file on the remote server."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": "The local path to save the downloaded file."
                },
                "password": {
                    "type": "string",
                    "title": "Password",
                    "description": "(Optional) Password for authentication (use key_filepath if possible).",
                    "format": "password"
                },
                "key_filepath": {
                    "type": "string",
                    "title": "SSH Key File Path",
                    "description": "(Optional) Path to the private SSH key for authentication."
                }
            },
            "required": ["host", "user", "remote_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        port = params.get("port", 22)
        user = params.get("user")
        password = params.get("password")
        key_filepath = params.get("key_filepath")
        remote_path = params.get("remote_path")
        output_path = Path(params.get("output_path"))

        if not all([host, user, remote_path, output_path]):
            raise ValueError("ScpExtractor requires 'host', 'user', 'remote_path', and 'output_path'.")
        if not password and not key_filepath:
            raise ValueError("ScpExtractor requires either 'password' or 'key_filepath'.")
        if inputs:
            print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print(f"Connecting to {host} as user '{user}' for SCP download...")
            ssh_client.connect(
                hostname=host, port=port, username=user,
                password=password, key_filename=key_filepath, timeout=30
            )
            with ssh_client.open_sftp() as sftp:
                print(f"Downloading '{remote_path}' to '{output_path}'...")
                sftp.get(remote_path, str(output_path))
                print("File downloaded successfully.")
        except Exception as e:
            print(f"SCP operation failed: {e}")
            if output_path.exists():
                output_path.unlink()
            raise
        finally:
            if ssh_client:
                ssh_client.close()

        container = DataContainer()
        container.add_file_path(output_path)
        container.metadata.update({'source_type': 'scp', 'remote_host': host, 'remote_path': remote_path})
        return container