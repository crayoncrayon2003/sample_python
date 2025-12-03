from fastapi import FastAPI, Body
from db_postgres import get_postgres_data, set_postgres_data, init_postgres, get_user
from db_mongo import get_mongo_data, set_mongo_data, get_orders
from external_rest import get_fixed_data, set_fixed_data
import uvicorn

app = FastAPI()

@app.on_event("startup")
def startup_event():
    init_postgres()

@app.get("/")
def root():
    return {"message": "REST API is running!"}

# PostgreSQL
@app.get("/postgres")
def read_postgres():
    return get_postgres_data()

@app.post("/postgres")
def write_postgres(data_list: list[dict] = Body(...)):
    return set_postgres_data(data_list)

# MongoDB
@app.get("/mongo")
def read_mongo():
    return get_mongo_data()

@app.post("/mongo")
def write_mongo(data_list: list[dict]):
    return set_mongo_data(data_list)

# Fixed API
@app.get("/fixed/{key}")
def read_fixed(key: str):
    return get_fixed_data(key)

@app.post("/fixed")
def write_fixed(payload: dict = Body(...)):
    """
    payload: {"key": "some_key", "value": some_value}
    """
    key = payload.get("key")
    value = payload.get("value")
    return set_fixed_data(key, value)

@app.get("/summary")
def summary():
    # Postgres
    user_data = get_user()

    # MongoDB
    orders_data = get_orders()

    # 外部 REST API
    fixed_data = get_fixed_data(key="default")
    # 結合して返す
    return {
        "user": user_data,
        "orders": orders_data,
        "fixedApi": {
            "service": fixed_data["service"],
            "value": fixed_data["value"]
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)