# scripts/plugins/extractors/from_http.py

import requests
import pandas as pd
from io import StringIO, BytesIO
from typing import Dict, Any, Optional

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class HttpExtractor(BaseExtractor):
    """
    Extracts data from a source via an HTTP GET request.
    """
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.url = self.params.get("url")
        self.format = SupportedFormats.from_string(self.params.get("format", "unknown"))
        self.method = self.params.get("method", "GET").upper()
        self.request_params = self.params.get("request_params")
        self.headers = self.params.get("headers")
        self.auth = tuple(self.params.get("auth")) if self.params.get("auth") else None
        self.pandas_options = self.params.get("pandas_options", {})
        if not self.url:
            raise ValueError("HttpExtractor requires a 'url' parameter.")

    def execute(self, inputs: Dict[str, Optional[DataContainer]] = None) -> DataContainer:
        if inputs:
            print(f"Warning: Extractor plugin received unexpected inputs: {list(inputs.keys())}")

        print(f"Fetching data via {self.method} from URL: {self.url}")
        try:
            response = requests.request(
                method=self.method, url=self.url, params=self.request_params,
                headers=self.headers, auth=self.auth, timeout=60
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            raise

        if self.format == SupportedFormats.CSV:
            df = pd.read_csv(StringIO(response.text), **self.pandas_options)
        elif self.format == SupportedFormats.JSON:
            try: df = pd.DataFrame(response.json(), **self.pandas_options)
            except (pd.errors.ParserError, AttributeError):
                df = pd.json_normalize(response.json(), **self.pandas_options)
        elif self.format == SupportedFormats.PARQUET:
            df = pd.read_parquet(BytesIO(response.content), **self.pandas_options)
        else:
            raise ValueError(f"Unsupported format '{self.format.value}' for HttpExtractor.")

        container = DataContainer(data=df)
        container.metadata['source_url'] = self.url
        container.metadata['source_format'] = self.format.value
        container.metadata['http_status_code'] = response.status_code
        print(f"Successfully fetched and parsed {len(df)} rows.")
        return container