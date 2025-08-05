# scripts/plugins/loaders/to_context_broker.py

import asyncio, aiohttp, json
from typing import Dict, Any, List, Optional
import pluggy

from scripts.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ContextBrokerLoader:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_context_broker"

    async def _send_batch(self, session: aiohttp.ClientSession, batch: List[Dict], batch_num: int):
        payload = json.dumps(batch) if self.ngsi_version == 'ld' else json.dumps({"actionType": "append", "entities": batch})
        try:
            async with session.post(self.endpoint, data=payload, headers=self.headers) as response:
                if response.status not in [200, 201, 204, 207]:
                    error_text = await response.text()
                    print(f"Batch {batch_num} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                print(f"Batch {batch_num} (size: {len(batch)}) processed with status {response.status}.")
        except aiohttp.ClientError as e:
            print(f"Batch {batch_num} failed with client error: {e}")
            raise

    async def _main(self, entities: List[Dict]):
        batches = [entities[i:i + self.batch_size] for i in range(0, len(entities), self.batch_size)]
        print(f"Split {len(entities)} entities into {len(batches)} batches.")
        conn = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_batch(session, batch, i) for i, batch in enumerate(batches)]
            await asyncio.gather(*tasks)

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        self.host = params.get("host")
        self.ngsi_version = params.get("ngsi_version", "ld").lower()
        self.data_column = params.get("data_column")
        self.batch_size = params.get("batch_size", 100)
        self.concurrency = params.get("concurrency", 5)
        if not self.host or not self.data_column:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'host' and 'data_column'.")
        if 'input_data' not in inputs or inputs['input_data'] is None:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires a single input named 'input_data'.")
        data = inputs['input_data']

        self.headers = {'Content-Type': 'application/json'}
        if params.get('service'): self.headers['Fiware-Service'] = params.get('service')
        if params.get('service_path'): self.headers['Fiware-ServicePath'] = params.get('service_path')
        if self.ngsi_version == 'v2': self.endpoint = f"{self.host.rstrip('/')}/v2/op/update"
        elif self.ngsi_version == 'ld':
            self.headers['Content-Type'] = 'application/ld+json'
            self.endpoint = f"{self.host.rstrip('/')}/ngsi-ld/v1/entityOperations/upsert"
        else: raise ValueError("Unsupported 'ngsi_version'.")

        if data.data is None or self.data_column not in data.data.columns:
            print(f"Warning: ContextBrokerLoader needs DataFrame with column '{self.data_column}'.")
            return
        try: entities = [json.loads(s) for s in data.data[self.data_column]]
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Failed to parse JSON in column '{self.data_column}'. Error: {e}")
        if not entities:
            print("No entities to load."); return

        print(f"Loading {len(entities)} NGSI-{self.ngsi_version} entities to {self.host}...")
        try:
            asyncio.run(self._main(entities))
            print("All batches processed successfully.")
        except Exception as e:
            print(f"An error occurred during Context Broker loading: {e}")
            raise