# scripts/plugins/loaders/to_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_scp"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        port = params.get("port", 22)
        user = params.get("user")
        password = params.get("password")
        key_filepath = params.get("key_filepath")
        remote_dir = params.get("remote_dir")
        if not host or not user or not remote_dir:
            raise ValueError("ScpLoader requires 'host', 'user', and 'remote_dir'.")
        if not password and not key_filepath:
            raise ValueError("ScpLoader requires either 'password' or 'key_filepath'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: ScpLoader received no file paths to upload.")
            return

        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print(f"Connecting to {host} as user '{user}' for SCP upload...")
            ssh_client.connect(
                hostname=host, port=port, username=user,
                password=password, key_filename=key_filepath, timeout=30
            )
            print("SSH connection established.")
            with ssh_client.open_sftp() as sftp:
                print(f"Uploading {len(data.file_paths)} files to remote '{remote_dir}'...")
                try: sftp.stat(remote_dir)
                except FileNotFoundError: sftp.mkdir(remote_dir)
                for file_path in data.file_paths:
                    if file_path.is_file():
                        remote_path = f"{remote_dir}/{file_path.name}"
                        print(f"  Uploading '{file_path.name}' to '{remote_path}'...")
                        sftp.put(str(file_path), remote_path)
            print("All files uploaded successfully.")
        except Exception as e:
            print(f"SCP operation failed: {e}")
            raise
        finally:
            if ssh_client: ssh_client.close(); print("SSH connection closed.")