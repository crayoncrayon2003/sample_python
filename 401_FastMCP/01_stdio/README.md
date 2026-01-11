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
> ctrl + c
```

The stdio version does not start the server independently. The MCP client starts the MCP server.
Therefore, use Ctrl+C to termination.

# MCP Client
## Oiginal Test Client for Testing
```
python mcp_client.py
```

