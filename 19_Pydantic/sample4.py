from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import Optional, List
from bson import ObjectId
import json

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, schema: dict) -> None:
        schema.update({"type": "string"})

class UserModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias='_id')
    name: str
    age: int
    password1: str
    password2: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    @field_validator('name')
    def name_must_contain_space(cls, v):
        if ' ' not in v:
            raise ValueError('must contain a space')
        return v.title()

    @field_validator('age')
    def age_must_greater_than_zero(cls, v):
        if (v < 0):
            raise ValueError('must contain a space')
        return v

    @field_validator('password2')
    def passwords_match(cls, v, values):
        if values.data and 'password1' in values.data and v != values.data['password1']:
            raise ValueError('passwords do not match')
        return v

def main():
    data = {
        'name': 'FirstName FamilyName',
        'age': 30,
        'password1': 'pass',
        'password2': 'pass',
    }

    try:
        userA = UserModel(**data)
        print(userA)
    except ValidationError as e:
        print(e.json())

    try:
        json.dumps(userA)
    except :
        print("json.dumps is error. therefore id type is ", type(userA.id))

    print('value={} , type={}'.format(userA,                            type(userA)))
    print('value={} , type={}'.format(userA.model_dump(by_alias=True),  type(userA.model_dump(by_alias=True))))
    print('value={} , type={}'.format(userA.model_dump_json(),          type(userA.model_dump_json())))

if __name__=='__main__':
    main()
