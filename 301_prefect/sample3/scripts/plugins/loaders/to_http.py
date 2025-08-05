# scripts/plugins/loaders/to_http.py

import asyncio
import aiohttp
import json
from typing import Dict, Any, List

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class HttpLoader(BaseLoader):
    """
    Loads data by sending it to an HTTP endpoint.

    This loader iterates through a DataFrame column (assumed to contain JSON
    strings), and sends each one as the body of an HTTP POST or PUT request
    to a specified URL. It uses `aiohttp` for efficient, concurrent requests.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with HTTP request parameters.

        Expected params:
            - url (str): The target URL to send requests to.
            - data_column (str): The column in the DataFrame containing the
              payload for each request (e.g., 'ngsi_entity').
            - method (str, optional): HTTP method, 'POST' or 'PUT'. Defaults to 'POST'.
            - headers (dict, optional): Common HTTP headers for all requests.
            - concurrency (int, optional): The number of concurrent requests to send.
              Defaults to 10.
            - stop_on_fail (bool, optional): If True, stops on the first failed
              request. Defaults to True.
        """
        super().__init__(params)
        self.url = self.params.get("url")
        self.data_column = self.params.get("data_column")
        self.method = self.params.get("method", "POST").upper()
        self.headers = self.params.get("headers", {})
        self.concurrency = self.params.get("concurrency", 10)
        self.stop_on_fail = self.params.get("stop_on_fail", True)

        if not self.url or not self.data_column:
            raise ValueError("HttpLoader requires 'url' and 'data_column' parameters.")
        if self.method not in ["POST", "PUT"]:
            raise ValueError("HttpLoader 'method' must be 'POST' or 'PUT'.")
        # Ensure Content-Type is set for JSON payloads
        if 'Content-Type' not in self.headers:
            self.headers['Content-Type'] = 'application/json'

    async def _send_request(self, session: aiohttp.ClientSession, payload: str, index: int):
        """Coroutine to send a single HTTP request."""
        try:
            async with session.request(self.method, self.url, data=payload.encode('utf-8'), headers=self.headers) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    print(f"Request {index} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status() # This will raise ClientResponseError
                
                print(f"Request {index} succeeded with status {response.status}.")
                return {'status': 'success', 'index': index, 'statusCode': response.status}
        except aiohttp.ClientError as e:
            print(f"Request {index} failed with client error: {e}")
            if self.stop_on_fail:
                raise # Propagate the error to stop all tasks
            return {'status': 'failed', 'index': index, 'error': str(e)}


    async def _main(self, payloads: List[str]):
        """Main coroutine to manage concurrent requests."""
        conn = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_request(session, payload, i) for i, payload in enumerate(payloads)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check for exceptions that were caught by asyncio.gather
        final_errors = [res for res in results if isinstance(res, Exception)]
        if final_errors:
            # If any task raised an unhandled exception, re-raise the first one.
            raise final_errors[0]


    def execute(self, data: DataContainer) -> None:
        """
        Sends the data from the specified column to the HTTP endpoint.
        """
        if data.data is None or self.data_column not in data.data.columns:
            print(f"Warning: HttpLoader requires a DataFrame with column '{self.data_column}'. Skipping.")
            return

        payloads = data.data[self.data_column].tolist()
        if not payloads:
            print("No data to load. Skipping HTTP requests.")
            return
            
        print(f"Sending {len(payloads)} HTTP {self.method} requests to {self.url} with concurrency {self.concurrency}...")
        
        try:
            # Run the main async function
            asyncio.run(self._main(payloads))
            print("All HTTP requests processed successfully.")
        except Exception as e:
            print(f"An error occurred during HTTP loading: {e}")
            # Re-raise to mark the Prefect task as failed
            raise