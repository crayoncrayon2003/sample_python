import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

STORE = {}

class SetRequest(BaseModel):
    key: str
    value: str

@app.post("/store/set")
def set_value(req: SetRequest):
    STORE[req.key] = req.value
    return {"status": "ok"}

@app.get("/store/get")
def get_value(key: str):
    return {
        "key": key,
        "value": STORE.get(key)
    }

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8001)