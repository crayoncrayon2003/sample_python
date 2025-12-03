from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()

_fixed_data_store = {}

class FixedDataItem(BaseModel):
    key: str
    value: dict

@app.get("/")
def root():
    return {"message": "Fixed API is running!"}

@app.post("/set_fixed")
def set_fixed_data(item: FixedDataItem):
    _fixed_data_store[item.key] = item.value
    return {"message": f"Data set for key '{item.key}'"}

@app.get("/get_fixed/{key}")
def get_fixed_data(key: str):
    data = _fixed_data_store.get(key)
    if not data:
        # デフォルト値を返す場合
        return {"service": "sample_service", "value": 123}
        # もしくはエラーにする場合
        # raise HTTPException(status_code=404, detail="Key not found")
    return data

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
