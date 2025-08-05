# scripts/plugins/extractors/from_http.py

import requests
import pandas as pd
from io import StringIO, BytesIO
from typing import Dict, Any

from .base import BaseExtractor
from scripts.core.data_container.container import DataContainer
from scripts.core.data_container.formats import SupportedFormats

class HttpExtractor(BaseExtractor):
    """
    Extracts data from a source via an HTTP GET request.

    This extractor can fetch data from web URLs or REST APIs. It supports
    common data formats like CSV and JSON and can handle various HTTP
    authentication methods and parameters.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the extractor with parameters for the HTTP request.

        Expected params:
            - url (str): The URL to fetch data from.
            - format (str): The format of the data ('csv', 'json', etc.).
            - method (str, optional): HTTP method to use. Defaults to 'GET'.
            - request_params (dict, optional): Dictionary of URL parameters
              (query string).
            - headers (dict, optional): Dictionary of HTTP headers to send.
            - auth (list, optional): List for basic authentication, e.g.,
              ['username', 'password'].
            - pandas_options (dict, optional): Options to pass to the pandas
              read function.
        """
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

    def execute(self) -> DataContainer:
        """
        Performs the HTTP request and processes the response into a DataFrame.

        Returns:
            DataContainer: A container with the fetched data.
        
        Raises:
            requests.RequestException: If the HTTP request fails.
            ValueError: If the response format is not supported.
        """
        print(f"Fetching data via {self.method} from URL: {self.url}")

        try:
            response = requests.request(
                method=self.method,
                url=self.url,
                params=self.request_params,
                headers=self.headers,
                auth=self.auth,
                timeout=60  # Set a reasonable timeout
            )
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()

        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            raise

        df: pd.DataFrame
        # Process the response content based on the specified format
        if self.format == SupportedFormats.CSV:
            # Use StringIO to treat the string response content as a file
            df = pd.read_csv(StringIO(response.text), **self.pandas_options)
        elif self.format == SupportedFormats.JSON:
            try:
                # Use read_json directly on the JSON response
                df = pd.DataFrame(response.json(), **self.pandas_options)
            except (pd.errors.ParserError, AttributeError):
                # Fallback for complex JSON, needs json_normalize
                # Here we assume the user specified normalize options in pandas_options
                df = pd.json_normalize(response.json(), **self.pandas_options)
        elif self.format == SupportedFormats.PARQUET:
            # Use BytesIO to treat the binary response content as a file
            df = pd.read_parquet(BytesIO(response.content), **self.pandas_options)
        else:
            raise ValueError(f"Unsupported format '{self.format.value}' for HttpExtractor.")

        container = DataContainer(data=df)
        container.metadata['source_url'] = self.url
        container.metadata['source_format'] = self.format.value
        container.metadata['http_status_code'] = response.status_code

        print(f"Successfully fetched and parsed {len(df)} rows.")
        return container