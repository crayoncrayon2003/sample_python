import json

def cast_value(value, target_type):
    if target_type == "float":
        return float(value)
    elif target_type == "int":
        return int(value)
    elif target_type == "str":
        return str(value)
    elif target_type == "TEXT":
        return str(value)
    return value

def convert_ngsi_to_json(ngsi_data, schema):
    json_data = {}
    for key, json_key in schema["mapping"]["ngsi_to_json"].items():
        property_name, _ = key.split(".")
        target_type = schema["typeMapping"].get(property_name, {}).get("json", "str")

        if property_name in ngsi_data and "value" in ngsi_data[property_name]:
            # using input values
            value = ngsi_data[property_name]["value"]
        else:
            # using default values
            value = schema.get("defaultValues", {}).get(property_name)

        # type conversion
        try:
            json_data[json_key] = cast_value(value, target_type)
        except ValueError as e:
            print(f"Type conversion failed for {json_key}: {e}")
    return json_data

def convert_json_to_ngsi(json_data, schema):
    ngsi_data = {}
    for json_key, key in schema["mapping"]["json_to_ngsi"].items():
        property_name, _ = key.split(".")
        source_type = schema["typeMapping"].get(property_name, {}).get("json", "str")
        target_type = schema["typeMapping"].get(property_name, {}).get("ngsi", "TEXT")

        if json_key in json_data:
            # using input values
            value = json_data[json_key]
        else:
            # using default values
            value = schema.get("defaultValues", {}).get(property_name)

        # type conversion
        try:
            value = cast_value(value, source_type)
            ngsi_data[property_name] = {
                "value": value,
                "type": target_type
            }
        except ValueError as e:
            print(f"Type conversion failed for {key}: {e}")

    return ngsi_data

def main():
    schema = {
        "entityType": "Room",
        "mapping": {
            "ngsi_to_json": {
                "temperature.value": "temperature",
                "humidity.value": "humidity"
            },
            "json_to_ngsi": {
                "temperature": "temperature.value",
                "humidity": "humidity.value"
            }
        },
        "typeMapping": {
            "temperature": {"ngsi": "Number", "json": "float"},
            "humidity"   : {"ngsi": "TEXT"  , "json": "str"}
        },
        "defaultValues": {
            "temperature": 20.0,
            "humidity"   : "50"
        }
    }

    source_ngsi_data = {
        "id": "room1",
        "type": "Room",
        "temperature": {
            "value": 23,
            # type  is lack
            # "type": "Number"
        }
        # humidity is lack
        # "humidity": {
        #     "value": "60",
        #     "type": "TEXT"
        # }
    }

    source_json_data = {
        "temperature": 23
        # humidity is lack
        # "humidity": 60
    }

    print("NGSI → JSON:")
    converted_to_json = convert_ngsi_to_json(source_ngsi_data, schema)
    print(json.dumps(converted_to_json, indent=2))

    print("JSON → NGSI:")
    converted_to_ngsi = convert_json_to_ngsi(source_json_data, schema)
    print(json.dumps(converted_to_ngsi, indent=2))

if __name__ == "__main__":
    main()
