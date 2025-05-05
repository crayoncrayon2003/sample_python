from pydantic import BaseModel, Field, ValidationError, validator
from typing import Optional
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
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

class UserModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias='_id')
    name: str
    age: int
    password1: str
    password2: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

    @validator('name')
    def name_must_contain_space(cls, v):
        if ' ' not in v:
            raise ValueError('must contain a space')
        return v.title()

    @validator('age')
    def age_must_greater_than_zero(cls, v):
        if v < 0:
            raise ValueError('age must be greater than zero')
        return v

    @validator('password2')
    def passwords_match(cls, v, values, **kwargs):
        if 'password1' in values and v != values['password1']:
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
    except TypeError as e:
        print(f"json.dumps is error. Therefore, id type is {type(userA.id)}")

    print(f'value={userA} , type={type(userA)}')
    print(f'value={userA.dict(by_alias=True)} , type={type(userA.dict(by_alias=True))}')
    print(f'value={userA.json()} , type={type(userA.json())}')

if __name__ == '__main__':
    main()
