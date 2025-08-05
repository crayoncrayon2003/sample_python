# scripts/plugins/loaders/to_http.py

import asyncio, aiohttp, json
from typing import Dict, Any, List, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_http"

    async def _send_request(self, session: aiohttp.ClientSession, payload: str, index: int):
        try:
            async with session.request(self.method, self.url, data=payload.encode('utf-8'), headers=self.headers) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    print(f"Request {index} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                print(f"Request {index} succeeded with status {response.status}.")
        except aiohttp.ClientError as e:
            print(f"Request {index} failed with client error: {e}")
            if self.stop_on_fail: raise

    async def _main(self, payloads: List[str]):
        conn = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_request(session, payload, i) for i, payload in enumerate(payloads)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        final_errors = [res for res in results if isinstance(res, Exception)]
        if final_errors: raise final_errors[0]

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        self.url = params.get("url")
        self.data_column = params.get("data_column")
        self.method = params.get("method", "POST").upper()
        self.headers = params.get("headers", {})
        self.concurrency = params.get("concurrency", 10)
        self.stop_on_fail = params.get("stop_on_fail", True)
        if not self.url or not self.data_column:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'url' and 'data_column'.")
        if self.method not in ["POST", "PUT"]: raise ValueError("'method' must be 'POST' or 'PUT'.")
        if 'Content-Type' not in self.headers: self.headers['Content-Type'] = 'application/json'
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        if data.data is None or self.data_column not in data.data.columns:
            print(f"Warning: HttpLoader needs DataFrame with column '{self.data_column}'. Skipping.")
            return
        payloads = data.data[self.data_column].tolist()
        if not payloads:
            print("No data to load. Skipping HTTP requests.")
            return

        print(f"Sending {len(payloads)} HTTP {self.method} requests to {self.url}...")
        try:
            asyncio.run(self._main(payloads))
            print("All HTTP requests processed successfully.")
        except Exception as e:
            print(f"An error occurred during HTTP loading: {e}")
            raise