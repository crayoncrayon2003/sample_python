import json

def transform_data(input_data, schema, direction):
    mapping = schema["mapping"][direction]
    type_mapping = schema.get("typeMapping", {})
    output_data = {}

    for source, target in mapping.items():
        # Access to nested attributes
        source_keys = source.split(".")
        value = input_data
        try:
            for key in source_keys:
                value = value[key]
        except (KeyError, TypeError):
            # attribute does not exist, it is set to None.
            value = None

        # type conversion
        if value is not None and direction == "json_to_ngsi":
            attribute_name = source_keys[0]
            if attribute_name in type_mapping:
                ngsi_type = type_mapping[attribute_name]
                if ngsi_type == "Number" and not isinstance(value, (int, float)):
                    value = float(value) if value.isdigit() else value

        # Assigning values
        target_keys = target.split(".")
        current = output_data
        for key in target_keys[:-1]:
            current = current.setdefault(key, {})
        current[target_keys[-1]] = value

    return output_data

def main():
    schema = {
        "entityType": "Room",
        "mapping": {
            "ngsi_to_json": {
                "id": "id",
                "type": "type",
                "temperature.value": "attributes.temperature",
                "temperature.type": "attributes.temperature_type",
                "humidity.value": "attributes.humidity",
                "humidity.type": "attributes.humidity_type"
            },
            "json_to_ngsi": {
                "id": "id",
                "type": "type",
                "attributes.temperature": "temperature.value",
                "attributes.temperature_type": "temperature.type",
                "attributes.humidity": "humidity.value",
                "attributes.humidity_type": "humidity.type"
            }
        },
        "typeMapping": {
            "temperature": "Number",
            "humidity": "Number"
        }
    }

    source_ngsi_data = {
        "id": "room1",
        "type": "Room",
        "temperature": {
            "value": 23,
            "type": "Number"
        },
        "humidity"   : {
            "value": 60,
            "type": "Number"
        }
    }

    source_json_data = {
        "id": "room1",
        "type": "Room",
        "attributes": {
            "temperature": 23,
            "temperature_type": "Number",
            "humidity": 60,
            "humidity_type": "Number"
        }
    }

    print("NGSI -> JSON:")
    converted_to_json = transform_data(source_ngsi_data, schema, "ngsi_to_json")
    print(json.dumps(converted_to_json, indent=2))

    print("JSON -> NGSI:")
    converted_to_ngsi = transform_data(source_json_data, schema, "json_to_ngsi")
    print(json.dumps(converted_to_ngsi, indent=2))

if __name__ == "__main__":
    main()