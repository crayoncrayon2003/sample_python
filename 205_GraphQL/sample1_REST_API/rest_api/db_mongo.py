from pymongo import MongoClient
import os

MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))

def get_mongo_data():
    client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}")
    db = client.demo
    return list(db.orders.find({}, {"_id": 0}))

def set_mongo_data(data_list):
    client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}")
    db = client.demo
    if data_list:
        db.orders.insert_many(data_list)
    return {"message": f"{len(data_list)} records inserted into MongoDB"}

def get_orders():
    return get_mongo_data()