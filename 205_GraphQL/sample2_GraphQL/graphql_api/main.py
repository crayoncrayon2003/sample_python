from fastapi import FastAPI
import uvicorn
from strawberry.fastapi import GraphQLRouter
from schema import schema
from db_postgres import init_postgres, insert_sample_user

app = FastAPI()
app.include_router(GraphQLRouter(schema), prefix="/graphql")

@app.on_event("startup")
def startup_event():
    init_postgres()
    insert_sample_user()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
