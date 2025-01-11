import os
from flask import Flask, request, jsonify
import pandas as pd
import avro.schema
import avro.io
import io
from apache_atlas.client.base_client import AtlasClient

# Flask Server
APP = Flask(__name__)

# Apache Atlas client
ATLAS_CLIENT = AtlasClient("http://localhost:21000", ("admin", "admin"))

# Load schema from Atlas
def get_entities_from_atlas(atlas_client: AtlasClient, schema_name, table_qualified_name):
    try:
        table_entity = atlas_client.entity.get_entity_by_attribute(
            type_name="hive_table",
            uniq_attributes={"qualifiedName": table_qualified_name}
        )
        table_attributes = table_entity["entity"]["attributes"]

        if table_attributes.get("name") != schema_name:
            raise ValueError(f"Schema name mismatch: Expected {schema_name}, Found {table_attributes.get('name')}")

        column_guids = [col["guid"] for col in table_attributes.get("columns", [])]
        schema = {
            "type": "record",
            "name": schema_name,
            "fields": []
        }

        for column_guid in column_guids:
            column_entity = atlas_client.entity.get_entity_by_guid(column_guid)
            column_name = column_entity["entity"]["attributes"]["name"]
            column_type = column_entity["entity"]["attributes"].get("type", "string").lower()
            avro_type = "string" if column_type == "string" else "int"

            schema["fields"].append({"name": column_name, "type": avro_type})

        return schema

    except Exception as e:
        print(f"Failed to retrieve schema from Atlas: {e}")
        return None

def sort_schema_fields(schema):
    sorted_fields = sorted(schema["fields"], key=lambda field: field["name"])
    sorted_schema = {
        "type": schema["type"],
        "name": schema["name"],
        "fields": sorted_fields,
    }
    return sorted_schema


def validate_data_with_schema(data, schema):
    schema_name = schema["name"]

    for record in data:
        for field in schema["fields"]:
            field_name = field["name"]
            expected_type = field["type"]

            if field_name not in record:
                raise ValueError(
                    f"In schema '{schema_name}', field '{field_name}' is missing in the record."
                )

            actual_value = record[field_name]
            if expected_type == "string":
                if not isinstance(actual_value, str):
                    record[field_name] = str(actual_value)
            elif expected_type == "int":
                if not isinstance(actual_value, int):
                    record[field_name] = int(actual_value)
    return True

def convert_to_target_type(value, target_type):
    try:
        if target_type == "string":
            return str(value)
        elif target_type == "int":
            return int(value)
        else:
            return value
    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to convert value '{value}' to type '{target_type}': {e}")

def schema_transform_avro(source_schema, target_schema, input_data):
    try:
        sorted_source_schema = sort_schema_fields(source_schema)
        sorted_target_schema = sort_schema_fields(target_schema)

        validate_data_with_schema(input_data, sorted_source_schema)

        output_data = [
            {
                field["name"]: (
                    convert_to_target_type(record[field["name"]], field["type"]) if field["name"] in record else None
                )
                for field in sorted_target_schema["fields"]
            }
            for record in input_data
        ]

        validate_data_with_schema(output_data, sorted_target_schema)

        return output_data

    except Exception as e:
        print(f"Error during schema validation or transformation: {e}")
        return None



# Flask REST API Endpoint
@APP.route('/transform', methods=['POST'])
def transform_data():
    try:
        # get request body
        input_json = request.get_json()
        input_table_source = input_json.get("table_source")
        input_schema_source_name = input_json.get("schema_source")
        input_table_target = input_json.get("table_target")
        input_schema_target_name = input_json.get("schema_target")
        input_data = input_json.get("data")

        if not all([input_table_source, input_schema_source_name, input_table_target, input_schema_target_name, input_data]):
            return jsonify({"error": "Missing required input parameters"}), 400

        # Load schema
        schema_source = get_entities_from_atlas(ATLAS_CLIENT, input_schema_source_name, input_table_source)
        schema_target = get_entities_from_atlas(ATLAS_CLIENT, input_schema_target_name, input_table_target)

        if not schema_source or not schema_target:
            return jsonify({"error": "Failed to retrieve schemas from Apache Atlas"}), 500


        transformed_data = schema_transform_avro(schema_source, schema_target, input_data)

        if transformed_data is None:
            return jsonify({"error": "Schema transformation failed"}), 500

        # respons body
        output_json = {
            "table_source": input_table_source,
            "schema_source": input_schema_source_name,
            "table_target": input_table_target,
            "schema_target": input_schema_target_name,
            "data": transformed_data
        }

        return jsonify(output_json), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# run Flask Server
def run_flask():
    APP.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", 5000)))

if __name__ == "__main__":
    run_flask()
