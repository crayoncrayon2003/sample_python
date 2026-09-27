from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class UserOrm(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(63), unique=True)
    age: Mapped[int]
    password1: Mapped[str] = mapped_column(String(255))
    password2: Mapped[str] = mapped_column(String(255))


class UserModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str = Field(max_length=63)
    age: int
    password1: str = Field(max_length=255)
    password2: str = Field(max_length=255)


def main():
    data = {
        "id": 1,
        "name": "FirstName FamilyName",
        "age": 30,
        "password1": "pass",
        "password2": "pass",
    }

    user_a = UserOrm(**data)
    print(user_a, type(user_a))

    user_model = UserModel.model_validate(user_a)
    print(user_model, type(user_model))


if __name__ == "__main__":
    main()