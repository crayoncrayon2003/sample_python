import uvicorn
from fastapi import FastAPI
import time

app = FastAPI()

@app.get("/hello")
def hello():
    return {"message": "Hello, world!"}

@app.get("/sleep")
def sleep():
    time.sleep(5)
    return {"message": "Hello World"}

def main():
    print("Start the Test Server.")
    print("Keep it running and proceed to the next step.")
    uvicorn.run(app, host="127.0.0.1", port=8080)

if __name__ == "__main__":
  main()