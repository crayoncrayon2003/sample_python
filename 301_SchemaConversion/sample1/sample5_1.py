from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import AtlasEntity, AtlasEntitiesWithExtInfo
from apache_atlas.model.typedef import AtlasTypesDef
from apache_atlas.utils           import type_coerce


def create_schema_entities_in_atlas(atlas_client, schema_name, schema_fields, schema_qualified_name):
    schema_guid = f"-{abs(hash(schema_name) % 10000)}"
    field_guids = [f"-{abs(hash(field['name']) % 10000)}" for field in schema_fields]

    # Define schema entity
    schema_entity = AtlasEntity()
    schema_entity.guid = schema_guid
    schema_entity.typeName = "avro_schema"
    schema_entity.attributes = {
        "name": schema_name,
        "qualifiedName": schema_qualified_name,
        "owner": "admin",
        "description": f"Avro schema for {schema_name}",
        "type": "avro_record",
        "namespace": schema_name,
        "fields": [],
    }

    # Define field entities
    field_entities = []
    for guid, field in zip(field_guids, schema_fields):
        field_entity = AtlasEntity()
        field_entity.guid = guid
        field_entity.typeName = "avro_field"
        field_entity.attributes = {
            "name": field["name"],
            # "type": field["type"],
            "qualifiedName": f"{schema_qualified_name}.{field['name']}",
            "owner": "admin",
            "table": {"guid": schema_guid},
        }
        field_entities.append(field_entity)
        schema_entity.attributes["fields"].append({"guid": field_entity.guid})

    # Combine schema and field entities
    entities_with_ext_info = AtlasEntitiesWithExtInfo()
    entities_with_ext_info.entities = [schema_entity] + field_entities

    # Debug: Print payload
    print("Payload sent to Atlas:", entities_with_ext_info)

    # Send to Atlas
    try:
        response = atlas_client.entity.create_entities(entities_with_ext_info)
        print(f"Entities for {schema_name} successfully created in Apache Atlas.")
    except Exception as e:
        print(f"Failed to create entities for {schema_name}: {e}")
        raise


# Load schema from Atlas
def get_schema_from_atlas(atlas_client, schema_name, schema_qualified_name):
    try:
        # Retrieve the schema entity by qualifiedName
        schema_entity = atlas_client.entity.get_entity_by_attribute(
            type_name="avro_schema",
            uniq_attributes={"qualifiedName": schema_qualified_name}
        )

        # Validate schema name
        schema_attributes = schema_entity["entity"]["attributes"]
        if schema_attributes["name"] != schema_name:
            print(f"Schema name mismatch. Expected: {schema_name}, Found: {schema_attributes['name']}")
            return None

        # Retrieve field GUIDs from the schema
        field_guids = [field_ref["guid"] for field_ref in schema_attributes.get("fields", [])]

        # Fetch field details
        schema_fields = []
        for field_guid in field_guids:
            field_entity = atlas_client.entity.get_entity_by_guid(field_guid)
            field_attributes = field_entity["entity"]["attributes"]
            schema_fields.append({"name": field_attributes["name"], "type": field_attributes["type"]})

        return schema_fields

    except Exception as e:
        print(f"Failed to retrieve schema '{schema_name}' (qualifiedName: '{schema_qualified_name}'): {e}")
        raise

# Show schema
def show_schema(schema, name):
    print(f"name : {name}:")
    print(f"type : {type(schema)}")
    for field in schema:
        print(f" - {field['name']}: {field['type']}")

def main():
    # Define Atlas client
    atlas_client = AtlasClient("http://localhost:21000", ("admin", "admin"))

    # Define schemas directly as dictionaries
    schema_source = [
        {"name": "Name", "type": "string"},
        {"name": "Age", "type": "int"},
    ]
    schema_target = [
        {"name": "Name", "type": "string"},
        {"name": "Age", "type": "string"},
    ]
    print("--------- post schema ---------")
    # Sort in alphabetical order
    schema_source = sorted(schema_source, key=lambda x: x["name"])
    schema_target = sorted(schema_target, key=lambda x: x["name"])
    show_schema(schema_source, "schema_source")
    show_schema(schema_target, "schema_target")

    # Register schemas in Apache Atlas
    create_schema_entities_in_atlas(atlas_client, "source_schema_avroschema", schema_source, "default.source_schema_avroschema@avro")
    create_schema_entities_in_atlas(atlas_client, "target_schema_avroschema", schema_target, "default.target_schema_avroschema@avro")


    # Retrieve schemas from Apache Atlas
    get_source_schema_fields = get_schema_from_atlas(atlas_client, "source_schema_avroschema", "default.source_schema_avroschema@avro")
    get_target_schema_fields = get_schema_from_atlas(atlas_client, "target_schema_avroschema", "default.target_schema_avroschema@avro")

    # Sort in alphabetical order
    get_source_schema_fields = sorted(get_source_schema_fields, key=lambda x: x["name"])
    get_target_schema_fields = sorted(get_target_schema_fields, key=lambda x: x["name"])
    show_schema(get_source_schema_fields, "get_schema_source_sparktable")
    show_schema(get_target_schema_fields, "get_schema_target_sparktable")

if __name__ == "__main__":
    main()
