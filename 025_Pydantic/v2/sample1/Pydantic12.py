from pydantic import (
    BaseModel,
    ValidationError,
    ValidationInfo,
    field_validator,
)

class UserModel(BaseModel):
    id: int
    name: str
    age: int
    password1: str
    password2: str

    @field_validator("name")
    @classmethod
    def name_must_contain_space(cls, value: str) -> str:
        if ' ' not in value:
            raise ValueError('must contain a space')
        return value.title()

    @field_validator("age")
    @classmethod
    def age_must_greater_than_zero(cls, value: int):
        if value < 0:
            raise ValueError('age must be greater than zero')
        return value

    @field_validator("password2")
    @classmethod
    def passwords_match(cls, value: str, info: ValidationInfo,
    ) -> str:
        password1 = info.data.get("password1")

        if password1 is not None and value != password1:
            raise ValueError("passwords do not match")

        return value

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
