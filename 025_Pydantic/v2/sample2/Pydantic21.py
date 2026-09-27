import os
from typing import Any

import yaml
from pydantic import BaseModel, create_model


ROOT = os.path.dirname(os.path.abspath(__file__))
OLD_SCHEMA = os.path.join(ROOT, "Pydantic21_old_schema.yml")
NEW_SCHEMA = os.path.join(ROOT, "Pydantic21_new_schema.yml")


def load_yaml(file_path: str) -> dict[str, Any]:
    with open(file_path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_pydantic_model(
    schema: dict[str, Any],
    model_name: str,
) -> type[BaseModel]:
    fields: dict[str, tuple[Any, Any]] = {}

    for field_name, field_info in schema["properties"].items():
        field_type = field_info["type"]

        if field_type == "string":
            fields[field_name] = (str, ...)

        elif field_type == "integer":
            fields[field_name] = (int, ...)

        elif field_type == "object":
            nested_model = create_pydantic_model(
                field_info,
                field_name.capitalize(),
            )
            fields[field_name] = (nested_model, ...)

        elif field_type == "array":
            item_info = field_info["items"]
            item_type = item_info["type"]

            if item_type == "string":
                fields[field_name] = (list[str], ...)

            elif item_type == "object":
                item_model = create_pydantic_model(
                    item_info,
                    field_name.capitalize(),
                )
                fields[field_name] = (list[item_model], ...)

        else:
            raise ValueError(
                f"unsupported field type: {field_type}"
            )

    return create_model(model_name, **fields)


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

    old_schema = load_yaml(OLD_SCHEMA)
    new_schema = load_yaml(NEW_SCHEMA)

    OldModel = create_pydantic_model(
        old_schema,
        "OldData",
    )
    NewModel = create_pydantic_model(
        new_schema,
        "NewData",
    )

    old_data = OldModel.model_validate(old_json)
    print(old_data.model_dump_json(indent=4))

    transform_data = {
        "fullName": old_data.name,
        "details": {
            "age": old_data.age,
            "location": (
                f"{old_data.address.street}, "
                f"{old_data.address.city}"
            ),
        },
        "contacts": {
            phone.type: phone.number
            for phone in old_data.phoneNumbers
        },
    }

    new_data = NewModel.model_validate(transform_data)
    print(new_data.model_dump_json(indent=4))


if __name__ == "__main__":
    main()