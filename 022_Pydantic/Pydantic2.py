from pydantic import BaseModel, ValidationError, field_validator
from typing import Optional

class UserModel(BaseModel):
    id: int
    name: str
    age: int
    password1: str
    password2: str

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
        return

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