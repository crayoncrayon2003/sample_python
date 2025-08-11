# backend/plugins/extractors/from_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpExtractor:
    """
    (File-based) Extracts a file from an FTP server and saves it to a local path.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_ftp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "title": "FTP Host",
                    "description": "Hostname or IP address of the FTP server."
                },
                "remote_path": {
                    "type": "string",
                    "title": "Remote File Path",
                    "description": "The full path to the file on the FTP server."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": "The local path to save the downloaded file."
                },
                "user": {
                    "type": "string",
                    "title": "Username",
                    "description": "(Optional) Username for FTP authentication."
                },
                "password": {
                    "type": "string",
                    "title": "Password",
                    "description": "(Optional) Password for FTP authentication.",
                    "format": "password"
                }
            },
            "required": ["host", "remote_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_path = params.get("remote_path")
        output_path = Path(params.get("output_path"))

        if not all([host, remote_path, output_path]):
            raise ValueError("FtpExtractor requires 'host', 'remote_path', and 'output_path' parameters.")
        if inputs:
            print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Connecting to FTP server at {host} to download file.")
        try:
            with ftplib.FTP(host) as ftp:
                ftp.login(user=user, passwd=password)
                print(f"Downloading '{remote_path}' to '{output_path}'...")
                with open(output_path, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {remote_path}', local_file.write)
                print("File downloaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            if output_path.exists():
                output_path.unlink()
            raise

        container = DataContainer()
        container.add_file_path(output_path)
        container.metadata.update({'source_type': 'ftp', 'ftp_host': host, 'remote_path': remote_path})
        return container