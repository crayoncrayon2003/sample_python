import os
import yaml
import json
from pydantic import BaseModel, create_model
from typing import Dict, Any, List, Optional

ROOT   = os.path.dirname(os.path.abspath(__file__))
OLD_SCHEMA = os.path.join(ROOT,"Pydantic6_old_schema.yml")
NEW_SCHEMA = os.path.join(ROOT,"Pydantic6_new_schema.yml")

def load_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def create_pydantic_model(schema: Dict[str, Any], model_name: str) -> BaseModel:
    fields = {}
    for field_name, field_info in schema['properties'].items():
        field_type = field_info['type']
        if field_type == 'string':
            fields[field_name] = (str, ...)
        elif field_type == 'integer':
            fields[field_name] = (int, ...)
        elif field_type == 'object':
            fields[field_name] = (create_pydantic_model(field_info, field_name.capitalize()), ...)
        elif field_type == 'array':
            item_type = field_info['items']['type']
            if item_type == 'string':
                fields[field_name] = (List[str], ...)
            elif item_type == 'object':
                fields[field_name] = (List[create_pydantic_model(field_info['items'], field_name.capitalize())], ...)
    return create_model(model_name, **fields)


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

    # load schema
    old_schema = load_yaml(OLD_SCHEMA)
    new_schema = load_yaml(NEW_SCHEMA)

    # create pydantic model
    OldModel = create_pydantic_model(old_schema, 'OldData')
    NewModel = create_pydantic_model(new_schema, 'NewModel')



    # create pydantic instance
    old_data = OldModel(**old_json)
    print(old_data.json(indent=4))

    # transform new data
    transform_data = {
        "fullName"  : old_data.name,
        "details"   : {
            "age"       : old_data.age,
            "location"  : f"{old_data.address.street}, {old_data.address.city}"
        },
        "contacts"  : {phone.type: phone.number for phone in old_data.phoneNumbers}
    }

    new_data = NewModel(**transform_data)
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
