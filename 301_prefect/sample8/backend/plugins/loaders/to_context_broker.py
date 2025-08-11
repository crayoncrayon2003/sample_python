# backend/plugins/loaders/to_context_broker.py

import asyncio, aiohttp, json
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ContextBrokerLoader:
    """
    (File-based) Loads NGSI entities from a JSON Lines file to a Context Broker.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_context_broker"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input NGSI JSONL Path",
                    "description": "The JSON Lines file containing NGSI entities to load."
                },
                "host": {
                    "type": "string",
                    "title": "Context Broker Host",
                    "description": "The base URL of the Context Broker (e.g., 'http://orion:1026')."
                },
                "ngsi_version": {
                    "type": "string",
                    "title": "NGSI Version",
                    "enum": ["v2", "ld"],
                    "default": "ld"
                },
                "service": {"type": "string", "title": "Fiware-Service (Optional)"},
                "service_path": {"type": "string", "title": "Fiware-ServicePath (Optional)"}
            },
            "required": ["input_path", "host"]
        }

    async def _send_batch(self, session: aiohttp.ClientSession, batch: List[Dict], batch_num: int):
        payload = json.dumps(batch) if self.ngsi_version == 'ld' else json.dumps({"actionType": "append", "entities": batch})
        try:
            async with session.post(self.endpoint, data=payload, headers=self.headers) as response:
                if response.status not in [200, 201, 204, 207]:
                    error_text = await response.text()
                    print(f"Batch {batch_num+1} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                print(f"Batch {batch_num+1} (size: {len(batch)}) processed with status {response.status}.")
        except aiohttp.ClientError as e:
            print(f"Batch {batch_num+1} failed with client error: {e}")
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
        input_path = Path(params.get("input_path"))
        self.host = params.get("host")
        self.ngsi_version = params.get("ngsi_version", "ld").lower()
        self.batch_size = params.get("batch_size", 100)
        self.concurrency = params.get("concurrency", 5)

        if not input_path or not self.host:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'host'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        self.headers = {'Content-Type': 'application/json'}
        if params.get('service'): self.headers['Fiware-Service'] = params.get('service')
        if params.get('service_path'): self.headers['Fiware-ServicePath'] = params.get('service_path')
        if self.ngsi_version == 'v2': self.endpoint = f"{self.host.rstrip('/')}/v2/op/update"
        elif self.ngsi_version == 'ld':
            self.headers['Content-Type'] = 'application/ld+json'
            self.endpoint = f"{self.host.rstrip('/')}/ngsi-ld/v1/entityOperations/upsert"

        print(f"Reading NGSI entities from '{input_path}' to load into Context Broker...")
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                entities = [json.loads(line) for line in f if line.strip()]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in '{input_path}': {e}")

        if not entities:
            print("No entities to load."); return None

        print(f"Loading {len(entities)} NGSI entities to {self.host}...")
        try:
            asyncio.run(self._main(entities))
            print("All batches processed successfully.")
        except Exception as e:
            print(f"An error occurred during Context Broker loading: {e}")
            raise
        return None