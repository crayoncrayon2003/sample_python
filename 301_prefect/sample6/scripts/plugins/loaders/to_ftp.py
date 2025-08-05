# scripts/plugins/loaders/to_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_ftp"

    def _upload_file(self, ftp: ftplib.FTP, local_path: Path, remote_dir: str) -> None:
        remote_filename = local_path.name
        print(f"  Uploading '{local_path.name}' to '{remote_dir}'...")
        with open(local_path, 'rb') as local_file:
            ftp.storbinary(f'STOR {remote_filename}', local_file)
        print(f"  Upload of '{remote_filename}' successful.")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_dir = params.get("remote_dir", "/")
        if not host: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'host'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if not data.file_paths:
            print("Warning: FtpLoader received no file paths to upload.")
            return

        print(f"Connecting to FTP at {host} to upload {len(data.file_paths)} files...")
        try:
            with ftplib.FTP(host, timeout=60) as ftp:
                ftp.login(user=user, passwd=password)
                print(f"Logged in as '{user or 'anonymous'}'.")
                if remote_dir != '/':
                    print(f"Changing remote directory to '{remote_dir}'...")
                    ftp.cwd(remote_dir)
                for file_path in data.file_paths:
                    if file_path.is_file(): self._upload_file(ftp, file_path, remote_dir)
            print("All files uploaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            raise