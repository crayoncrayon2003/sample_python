from pydantic import BaseModel, Field
from typing import List

# Old Data json schema
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
    phoneNumbers: List[PhoneNumber]

# New Data json schema
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
    # old data
    old_json = {
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "Anytown"
        },
        "phoneNumbers": [
            {"type": "home", "number": "123-456-7890"},
            {"type": "work", "number": "987-654-3210"}
        ]
    }

    # json to Pydantic
    old_data = OldData(**old_json)

    # transform new data
    new_data = NewData(
        fullName=old_data.name,
        details=Details(
            age=old_data.age,
            location=f"{old_data.address.street}, {old_data.address.city}"
        ),
        contacts=Contacts(
            home=[pn.number for pn in old_data.phoneNumbers if pn.type == "home"][0],
            work=[pn.number for pn in old_data.phoneNumbers if pn.type == "work"][0]
        )
    )

    print(new_data.json(indent=4))
    # new_data = {
    #     "fullName": "John Doe",
    #     "details": {
    #         "age": 30,
    #         "location": "123 Main St, Anytown"
    #     },
    #     "contacts": {
    #         "home": "123-456-7890",
    #         "work": "987-654-3210"
    #     }
    # }

if __name__=='__main__':
    main()
