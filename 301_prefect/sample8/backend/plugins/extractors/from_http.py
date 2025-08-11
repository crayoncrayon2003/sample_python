# backend/plugins/extractors/from_http.py

import requests
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpExtractor:
    """
    (File-based) Extracts data from an HTTP source and saves it to a local file.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "title": "Source URL",
                    "description": "The URL to download the data file from."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": "The local path to save the downloaded file."
                },
                "method": {
                    "type": "string",
                    "title": "HTTP Method",
                    "description": "The HTTP method to use.",
                    "enum": ["GET", "POST"],
                    "default": "GET"
                }
            },
            "required": ["url", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        output_path = Path(params.get("output_path"))
        method = params.get("method", "GET").upper()
        request_params = params.get("request_params")
        headers = params.get("headers")
        auth = tuple(params.get("auth")) if params.get("auth") else None

        if not url or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'url' and 'output_path' parameters.")
        if inputs:
            print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        print(f"Fetching data via {method} from '{url}' and saving to '{output_path}'")
        try:
            with requests.request(
                method=method, url=url, params=request_params,
                headers=headers, auth=auth, timeout=60, stream=True
            ) as response:
                response.raise_for_status()

                output_path.parent.mkdir(parents=True, exist_ok=True)

                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            print("File downloaded successfully.")
        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            raise

        container = DataContainer()
        container.add_file_path(output_path)
        container.metadata['source_url'] = url

        return container