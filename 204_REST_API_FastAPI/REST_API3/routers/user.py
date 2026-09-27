from fastapi import APIRouter, Body
from pydantic import BaseModel, Field
from pydantic_core import core_schema
import json
from typing import Optional, List
from bson.json_util import dumps, loads
from pymongo import MongoClient
from bson import ObjectId

class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        return core_schema.no_info_plain_validator_function(
            cls.validate,
            serialization=core_schema.to_string_ser_schema(when_used='json'),
        )

    @classmethod
    def validate(cls, v):
        if not isinstance(v, (str, ObjectId)) or not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        return {'type': 'string'}

class UserModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias='_id')
    name: str
    age : int

# Custom encoder
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


router = APIRouter()

client = MongoClient('mongodb://0.0.0.0:27017/')
DB = client.UserModel
try:
    CO = DB.my_collection.drop()
except:
    pass
CO = DB.my_collection

# get user
@router.get('/users')
def get_users():
    users = []
    for user in CO.find():
        users.append(json.loads(json.dumps(user, cls=JSONEncoder)))
    return {'users': users}

# add user
@router.post('/users')
def get_user(user: UserModel):
    # Check id
    if hasattr(user, 'id'):
        # del user id
        delattr(user, 'id')

    CO.insert_one(user.model_dump(by_alias=True))

    return {'users': user}

# del user
@router.delete('/users/{index}')
def delete_user(index: str):
    CO.delete_one( {'_id': ObjectId(index)} )

    users = []
    for user in CO.find():
        users.append(UserModel(**user))
    return {'users': users}