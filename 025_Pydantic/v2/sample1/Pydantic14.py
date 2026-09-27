from typing import Annotated

from bson import ObjectId
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    ValidationError,
    ValidationInfo,
    WithJsonSchema,
    field_validator,
)


def validate_object_id(value: str | ObjectId) -> ObjectId:
    if isinstance(value, ObjectId):
        return value

    if not ObjectId.is_valid(value):
        raise ValueError("invalid ObjectId")

    return ObjectId(value)


PyObjectId = Annotated[
    ObjectId,
    BeforeValidator(validate_object_id),
    PlainSerializer(
        lambda value: str(value),
        return_type=str,
        when_used="json",
    ),
    WithJsonSchema({"type": "string"}, mode="validation"),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


class UserModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
    )

    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    name: str
    age: int
    password1: str
    password2: str

    @field_validator("name")
    @classmethod
    def name_must_contain_space(cls, value: str) -> str:
        if " " not in value:
            raise ValueError("must contain a space")
        return value.title()

    @field_validator("age")
    @classmethod
    def age_must_be_zero_or_greater(cls, value: int) -> int:
        if value < 0:
            raise ValueError("age must be zero or greater")
        return value

    @field_validator("password2")
    @classmethod
    def passwords_match(
        cls,
        value: str,
        info: ValidationInfo,
    ) -> str:
        password1 = info.data.get("password1")

        if password1 is not None and value != password1:
            raise ValueError("passwords do not match")

        return value


def main():
    data = {
        "name": "FirstName FamilyName",
        "age": 30,
        "password1": "pass",
        "password2": "pass",
    }

    try:
        user_a = UserModel.model_validate(data)
    except ValidationError as error:
        print(error.json(indent=2))
        return

    python_data = user_a.model_dump(by_alias=True)
    json_data = user_a.model_dump_json(by_alias=True)

    print(f"value={user_a}, type={type(user_a)}")
    print(f"value={python_data}, type={type(python_data)}")
    print(f"value={json_data}, type={type(json_data)}")


if __name__ == "__main__":
    main()