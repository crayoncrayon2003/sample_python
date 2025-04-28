import uvicorn
from fastapi import FastAPI, HTTPException
import time

app = FastAPI()
counter = {"flaky": 0}

@app.get("/hello")
def hello():
    return {"message": "Hello, world!"}

@app.get("/sleep")
def sleep():
    time.sleep(5)
    return {"message": "Hello World"}

@app.get("/flaky")
def flaky():
    counter["flaky"] += 1
    if counter["flaky"] % 5 == 0:
        return {"message": "This is a successful response!"}
    else:
        raise HTTPException(status_code=503, detail="Service Unavailable")

def main():
    print("Start the Test Server.")
    print("Keep it running and proceed to the next step.")
    uvicorn.run(app, host="127.0.0.1", port=8080)

if __name__ == "__main__":
  main()