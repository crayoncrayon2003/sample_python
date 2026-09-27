from pydantic import BaseModel


# Old data schema
class PhoneNumber(BaseModel):
    type: str
    number: str


class Address(BaseModel):
    street: str
    city: str


class OldData(BaseModel):
    name: str
    age: int
    address: Address
    phoneNumbers: list[PhoneNumber]


# New data schema
class Contacts(BaseModel):
    home: str
    work: str


class Details(BaseModel):
    age: int
    location: str


class NewData(BaseModel):
    fullName: str
    details: Details
    contacts: Contacts


def main():
    old_json = {
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
        },
        "phoneNumbers": [
            {
                "type": "home",
                "number": "123-456-7890",
            },
            {
                "type": "work",
                "number": "987-654-3210",
            },
        ],
    }

    # 辞書をPydanticモデルへ変換
    old_data = OldData.model_validate(old_json)

    phone_numbers = {
        phone.type: phone.number
        for phone in old_data.phoneNumbers
    }

    # 新しいデータ構造へ変換
    new_data = NewData(
        fullName=old_data.name,
        details=Details(
            age=old_data.age,
            location=(
                f"{old_data.address.street}, "
                f"{old_data.address.city}"
            ),
        ),
        contacts=Contacts(
            home=phone_numbers["home"],
            work=phone_numbers["work"],
        ),
    )

    print(new_data.model_dump_json(indent=4))


if __name__ == "__main__":
    main()