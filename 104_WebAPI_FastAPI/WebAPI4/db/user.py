from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
DB = client.UserModel # database名がUserModel
CO = DB.my_collection.drop()
CO = DB.my_collection # collection名がmy_collection)