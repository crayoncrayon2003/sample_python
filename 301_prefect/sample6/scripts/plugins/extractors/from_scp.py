# scripts/plugins/extractors/from_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_scp"

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
        local_dir = Path(params.get("local_dir", "./temp/scp/"))
        if not host or not user or not remote_path:
            raise ValueError("ScpExtractor requires 'host', 'user', and 'remote_path'.")
        if not password and not key_filepath:
            raise ValueError("ScpExtractor requires either 'password' or 'key_filepath'.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(remote_path).name
        local_filepath = local_dir / remote_filename
        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print(f"Connecting to {host} as user '{user}'...")
            ssh_client.connect(
                hostname=host, port=port, username=user,
                password=password, key_filename=key_filepath, timeout=30
            )
            print("SSH connection established.")
            with ssh_client.open_sftp() as sftp:
                print(f"Downloading '{remote_path}' to '{local_filepath}' via SCP...")
                sftp.get(remote_path, str(local_filepath))
                print("File downloaded successfully.")
        except Exception as e:
            print(f"SCP operation failed: {e}")
            if local_filepath.exists(): local_filepath.unlink()
            raise
        finally:
            if ssh_client: ssh_client.close(); print("SSH connection closed.")

        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata.update({'source_type': 'scp', 'remote_host': host, 'remote_path': remote_path, 'local_path': str(local_filepath)})
        return container