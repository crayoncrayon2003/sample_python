# Creating Virtual Environment
```
$ python3.12 -m venv env
```

# Activate Virtual Environment
```
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools
(env) $ pip install -r requirements.txt
```

# Test REST-API
## Run Backend Environment
```
cd sample1_REST_API
docker compose up -d
```

## Run REST-API
```
cd rest_api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Call REST-API
### Set postgres
```
curl -X POST http://localhost:8000/postgres \
-H "Content-Type: application/json" \
-d '[
    {"id": 1, "item": "apple", "quantity": 10},
    {"id": 2, "item": "banana", "quantity": 5}
]'
```

### Get postgres
```
curl http://localhost:8000/postgres
```

### Set mongo
```
curl -X POST http://localhost:8000/mongo \
-H "Content-Type: application/json" \
-d '[
    {"id": 1, "item": "apple", "quantity": 10},
    {"id": 2, "item": "banana", "quantity": 5}
]'
```

### Get mongo
```
curl http://localhost:8000/mongo
```

### Set REST-API
```
curl -X POST http://localhost:8000/fixed \
-H "Content-Type: application/json" \
-d '{"key": "greeting", "value": {"msg": "Hello via main"}}'
```

### Get REST-API
```
curl http://localhost:8000/fixed/greeting
```

### GET summary
```
curl -X GET http://localhost:8000/summary
```


# Test GraphQL-API
## Run Backend Environment
```
cd sample2_GraphQL
docker compose up -d
```

## Run GraphQL-API
```
cd graphql_api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Call REST-API
```
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ summary { user { name } orders { item quantity } fixedApi { service value } } }"}'
```

```
curl "http://localhost:8000/graphql?query=%7Bsummary%7Buser%7Bname%7Dorders%7Bitem%20quantity%7DfixedApi%7Bservice%20value%7D%7D%7D"
```