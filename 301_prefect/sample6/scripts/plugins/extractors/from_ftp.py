# scripts/plugins/extractors/from_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_ftp"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_path = params.get("remote_path")
        local_dir = Path(params.get("local_dir", "./temp/ftp/"))
        if not all([host, remote_path, local_dir]):
            raise ValueError("FtpExtractor requires 'host', 'remote_path', and 'local_dir'.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        local_dir.mkdir(parents=True, exist_ok=True)
        remote_filename = Path(remote_path).name
        local_filepath = local_dir / remote_filename
        print(f"Connecting to FTP server at {host}...")
        try:
            with ftplib.FTP(host) as ftp:
                ftp.login(user=user, passwd=password)
                print(f"Logged in as '{user or 'anonymous'}'.")
                print(f"Downloading '{remote_path}' to '{local_filepath}'...")
                with open(local_filepath, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {remote_path}', local_file.write)
                print("File downloaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            if local_filepath.exists(): local_filepath.unlink()
            raise

        container = DataContainer()
        container.add_file_path(local_filepath)
        container.metadata.update({'source_type': 'ftp', 'ftp_host': host, 'remote_path': remote_path, 'local_path': str(local_filepath)})
        return container