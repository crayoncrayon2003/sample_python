from pydantic import BaseModel, ValidationError, Field
from pydantic.class_validators import validator
from typing import Optional

class UserModel(BaseModel):
    id: int
    name: str
    age: int
    password1: str
    password2: str

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
    def passwords_match(cls, v, values):
        if 'password1' in values and v != values['password1']:
            raise ValueError('passwords do not match')
        return v

def main():
    data = {
        'id': 1,
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

if __name__ == '__main__':
    main()
