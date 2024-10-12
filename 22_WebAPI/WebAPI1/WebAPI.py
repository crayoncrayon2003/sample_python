import uvicorn
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
  return {"message": "Hello World"}

def main():
  uvicorn.run(app, host="127.0.0.1", port=5049)

if __name__ == "__main__":
  main()