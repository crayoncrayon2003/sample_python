# scripts/plugins/extractors/from_http.py

import requests
import pandas as pd
from io import StringIO, BytesIO
from typing import Dict, Any, Optional
import pluggy

from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http"

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        file_format = SupportedFormats.from_string(params.get("format", "unknown"))
        method = params.get("method", "GET").upper()
        request_params = params.get("request_params")
        headers = params.get("headers")
        auth = tuple(params.get("auth")) if params.get("auth") else None
        pandas_options = params.get("pandas_options", {})
        if not url: raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a 'url' parameter.")
        if inputs: print(f"Warning: Extractor plugin '{self.get_plugin_name()}' received unexpected inputs.")

        print(f"Fetching data via {method} from URL: {url}")
        try:
            response = requests.request(
                method=method, url=url, params=request_params,
                headers=headers, auth=auth, timeout=60
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            raise

        if file_format == SupportedFormats.CSV:
            df = pd.read_csv(StringIO(response.text), **pandas_options)
        elif file_format == SupportedFormats.JSON:
            try: df = pd.DataFrame(response.json(), **pandas_options)
            except (pd.errors.ParserError, AttributeError):
                df = pd.json_normalize(response.json(), **pandas_options)
        elif file_format == SupportedFormats.PARQUET:
            df = pd.read_parquet(BytesIO(response.content), **pandas_options)
        else:
            raise ValueError(f"Unsupported format '{file_format.value}' for HttpExtractor.")

        container = DataContainer(data=df)
        container.metadata['source_url'] = url
        container.metadata['source_format'] = file_format.value
        container.metadata['http_status_code'] = response.status_code
        print(f"Successfully fetched and parsed {len(df)} rows.")
        return container