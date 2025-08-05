# scripts/plugins/loaders/to_context_broker.py

import asyncio
import aiohttp
import json
from typing import Dict, Any, List

from .base import BaseLoader
from scripts.core.data_container.container import DataContainer

class ContextBrokerLoader(BaseLoader):
    """
    Loads NGSI entities into a FIWARE Context Broker.

    This loader takes a column of NGSI entities (as JSON strings), bundles
    them into batches, and sends them to the Context Broker's batch
    operations endpoint for efficient bulk creation or update.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the loader with Context Broker connection details.

        Expected params:
            - host (str): The base URL of the Context Broker (e.g., 'http://orion:1026').
            - ngsi_version (str, optional): 'v2' or 'ld'. Defaults to 'ld'.
            - data_column (str): The column containing the NGSI entity JSON strings.
            - service (str, optional): The `Fiware-Service` header value.
            - service_path (str, optional): The `Fiware-ServicePath` header value.
            - batch_size (int, optional): Number of entities per batch request.
              Defaults to 100.
            - concurrency (int, optional): Number of concurrent batch requests.
              Defaults to 5.
        """
        super().__init__(params)
        self.host = self.params.get("host")
        self.ngsi_version = self.params.get("ngsi_version", "ld").lower()
        self.data_column = self.params.get("data_column")
        self.batch_size = self.params.get("batch_size", 100)
        self.concurrency = self.params.get("concurrency", 5)
        
        if not self.host or not self.data_column:
            raise ValueError("ContextBrokerLoader requires 'host' and 'data_column' parameters.")

        self.headers = {'Content-Type': 'application/json'}
        if self.params.get('service'):
            self.headers['Fiware-Service'] = self.params.get('service')
        if self.params.get('service_path'):
            self.headers['Fiware-ServicePath'] = self.params.get('service_path')

        if self.ngsi_version == 'v2':
            self.endpoint = f"{self.host.rstrip('/')}/v2/op/update"
        elif self.ngsi_version == 'ld':
            self.headers['Content-Type'] = 'application/ld+json'
            self.endpoint = f"{self.host.rstrip('/')}/ngsi-ld/v1/entityOperations/upsert"
        else:
            raise ValueError("Unsupported 'ngsi_version'. Must be 'v2' or 'ld'.")

    async def _send_batch(self, session: aiohttp.ClientSession, batch: List[Dict], batch_num: int):
        """Coroutine to send a single batch of entities."""
        payload = None
        if self.ngsi_version == 'v2':
            payload = json.dumps({"actionType": "append", "entities": batch})
        else: # ngsi-ld
            payload = json.dumps(batch)
            
        try:
            async with session.post(self.endpoint, data=payload, headers=self.headers) as response:
                if response.status not in [200, 201, 204, 207]: # 207 Multi-Status
                    error_text = await response.text()
                    print(f"Batch {batch_num} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                
                print(f"Batch {batch_num} (size: {len(batch)}) processed with status {response.status}.")
                return {'status': 'success', 'batch_num': batch_num, 'statusCode': response.status}
        except aiohttp.ClientError as e:
            print(f"Batch {batch_num} failed with client error: {e}")
            raise

    async def _main(self, entities: List[Dict]):
        """Main coroutine to manage concurrent batch requests."""
        # Create batches
        batches = [entities[i:i + self.batch_size] for i in range(0, len(entities), self.batch_size)]
        print(f"Split {len(entities)} entities into {len(batches)} batches of size up to {self.batch_size}.")

        conn = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_batch(session, batch, i) for i, batch in enumerate(batches)]
            await asyncio.gather(*tasks)

    def execute(self, data: DataContainer) -> None:
        if data.data is None or self.data_column not in data.data.columns:
            print(f"Warning: ContextBrokerLoader requires a DataFrame with column '{self.data_column}'. Skipping.")
            return

        # Convert JSON strings to dictionary objects
        try:
            entities = [json.loads(s) for s in data.data[self.data_column]]
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Failed to parse JSON in column '{self.data_column}'. Ensure it contains valid JSON strings. Error: {e}")

        if not entities:
            print("No entities to load. Skipping.")
            return
            
        print(f"Loading {len(entities)} NGSI-{self.ngsi_version} entities to Context Broker at {self.host}...")
        
        try:
            asyncio.run(self._main(entities))
            print("All batches processed successfully.")
        except Exception as e:
            print(f"An error occurred during Context Broker loading: {e}")
            raise