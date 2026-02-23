# Virtual Environment
## Creating Virtual Environment
```
python3.10 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) pip install -r requirements.txt
```
## Deactivate Virtual Environment
```
(env) $ deactivate
```

# REST API Server
## run API Server
```
python rest_api.py
```

## check API Server
### document
```
http://localhost:8001/docs
```

### check rest api
#### psot data
```
curl -X POST http://localhost:8001/store/set -H "Content-Type: application/json" -d '{"key":"foo","value":"bar"}'
```

#### get data
```
curl "http://localhost:8001/store/get?key=foo"
```

# MCP Server
## run MCP Server
```
python mcp_server.py
```

### prosess1 : SEE Session Start
```
curl -N http://localhost:8000/sse

> event: endpoint
> data: /messages/?session_id=734f36095bf84bf78298dae6555ce6bc
```

### prosess2 : initialize
```
curl -X POST "http://localhost:8000/messages/?session_id=734f36095bf84bf78298dae6555ce6bc" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "capabilities": {},
      "clientInfo": {
        "name": "test-client",
        "version": "1.0.0"
      }
    }
  }'
```

### prosess2 : get tool list
```
curl -X POST "http://localhost:8000/messages/?session_id=734f36095bf84bf78298dae6555ce6bc" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

### prosess2 : call store_set
```
curl -X POST "http://localhost:8000/messages/?session_id=734f36095bf84bf78298dae6555ce6bc" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "store_set",
      "arguments": {
        "key": "username",
        "value": "Alice"
      }
    }
  }'
```

### prosess2 : call store_get
```
curl -X POST "http://localhost:8000/messages/?session_id=734f36095bf84bf78298dae6555ce6bc" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 4,
    "method": "tools/call",
    "params": {
      "name": "store_get",
      "arguments": {
        "key": "username"
      }
    }
  }'
```

### prosess2 : SSE Session End
```
curl -X POST "http://localhost:8000/messages/?session_id=734f36095bf84bf78298dae6555ce6bc" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 99,
    "method": "notifications/cancelled"
  }'
```

# MCP Client
## Oiginal Test Client for Testing
```
python mcp_client.py
```

## Setting for VSCode Extension
.vscode/mcp.json
```
{
  "servers": {
    "my-local-store": {
      "type": "sse",
      "url": "http://localhost:8001/sse"
    }
  }
}
```

## Continue for VSCode Extension
For example, There is a "Continue" extension for VSCode.
Call the MCP server from “Continue”

```
{
  "models": [
    {
      "title": "Claude via MCP",
      "provider": "anthropic",
      "model": "claude-sonnet-4-20250514",
      "apiKey": "your-api-key"
    }
  ],
  "mcpServers": [
    {
      "name": "store-server",
      "transport": {
        "type": "sse",
        "url": "http://localhost:8000/sse"
      }
    }
  ]
}
```