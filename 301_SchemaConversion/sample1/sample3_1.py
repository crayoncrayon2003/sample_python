from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import AtlasEntity, AtlasEntitiesWithExtInfo
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def spark_type_to_atlas_type(spark_type):
    type_mapping = {
        "StringType": "string",
        "IntegerType": "int",
    }
    return type_mapping.get(type(spark_type).__name__, "string")

def atlas_type_to_spark_type(atlas_type):
    type_mapping = {
        "string": StringType(),
        "int": IntegerType(),
    }
    return type_mapping.get(atlas_type.lower(), StringType())

def create_schema_entities_in_atlas(atlas_client, schema_name, schema_fields, table_qualified_name):
    # Provisional GUIDs for table and columns
    table_guid = f"-{hash(schema_name) % 10000}"
    column_guids = [f"-{hash(field.name) % 10000}" for field in schema_fields.fields]

    # Define table entity
    table_entity = AtlasEntity()
    table_entity.guid = table_guid
    table_entity.typeName = "spark_table"
    table_entity.attributes = {
        "name": schema_name,
        "qualifiedName": table_qualified_name,
        "owner": "admin",
        "description": f"Schema for Spark table {schema_name}",
        "columns": [{"guid": guid} for guid in column_guids]
    }

    # Define column entities
    column_entities = []
    for guid, field in zip(column_guids, schema_fields.fields):
        column_entity = AtlasEntity()
        column_entity.guid = guid
        column_entity.typeName = "spark_column"
        column_entity.attributes = {
            "name": field.name,
            "qualifiedName": f"{table_qualified_name}.{field.name}",
            "type": type(field.dataType).__name__,
            #"type": spark_type_to_atlas_type(field.dataType),
            "owner": "admin",
            "table": {"guid": table_guid}
        }
        column_entities.append(column_entity)

    # Combine entities
    entities_with_ext_info = AtlasEntitiesWithExtInfo()
    entities_with_ext_info.entities = [table_entity] + column_entities

    # Send to Atlas
    try:
        response = atlas_client.entity.create_entities(entities_with_ext_info)
        print(f"Atlas create_entities response: {response}")
    except Exception as e:
        print(f"Failed to create entities for {schema_name}: {e}")
        raise

# Load schema from Atlas
def get_entities_from_atlas(atlas_client, schema_name, table_qualified_name):
    try:
        # Get table info
        table_entity = atlas_client.entity.get_entity_by_attribute(
            type_name="spark_table",
            uniq_attributes={"qualifiedName": table_qualified_name}
        )
        table_attributes = table_entity["entity"]["attributes"]

        if table_attributes.get("name") != schema_name:
            print(f"Schema name mismatch. Expected: {schema_name}, Found: {table_attributes.get('name')}")
            return None

        # Get columns info
        column_guids = table_entity.referredEntities
        struct_fields = []

        # Get details for column entities
        for column_guid in column_guids:
            column_entity = atlas_client.entity.get_entity_by_guid(column_guid)
            column_name = column_entity["entity"]["attributes"].get("name", "string")
            column_type = column_entity["entity"]["attributes"].get("type", "StringType")
            spark_type_class = globals().get(column_type, StringType)
            # spark_type_class=atlas_type_to_spark_type(column_type)
            struct_fields.append(StructField(column_name, spark_type_class(), True))

        # Create schema
        return StructType(struct_fields)
    except Exception as e:
        print(f"Failed to retrieve schema for '{schema_name}' (table: '{table_qualified_name}'):", e)
        raise

# Show schema
def show_schema(schema, name):
    print(f"name : {name}:")
    print(f"type : {type(schema)}")
    for field in schema.fields:
        print(f" - {field.name}: {field.dataType}")


def main():
    # Define Atlas client
    atlas_client = AtlasClient("http://localhost:21000", ("admin", "admin"))

    # Define schemas
    schema_source = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])
    schema_target = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", StringType(), True)
    ])

    print("--------- post schema ---------")
    # Sort in alphabetical order
    schema_source = StructType(sorted(schema_source.fields, key=lambda field: field.name))
    schema_target = StructType(sorted(schema_target.fields, key=lambda field: field.name))
    show_schema(schema_source, "schema_source")
    show_schema(schema_target, "schema_target")

    # Register schemas in Apache Atlas
    create_schema_entities_in_atlas(atlas_client, "source_schema_sparktable", schema_source, "default.source_schema_sparktable@spark")
    create_schema_entities_in_atlas(atlas_client, "target_schema_sparktable", schema_target, "default.target_schema_sparktable@spark")

    # Retrieve and print entities from Apache Atlas
    get_schema_source = get_entities_from_atlas(atlas_client, "source_schema_sparktable", "default.source_schema_sparktable@spark")
    get_schema_target = get_entities_from_atlas(atlas_client, "target_schema_sparktable", "default.target_schema_sparktable@spark")

    print("--------- get schema ---------")
    # Sort in alphabetical order
    get_schema_source = StructType(sorted(get_schema_source.fields, key=lambda field: field.name))
    get_schema_target = StructType(sorted(get_schema_target.fields, key=lambda field: field.name))
    show_schema(get_schema_source, "get_schema_source_sparktable")
    show_schema(get_schema_target, "get_schema_target_sparktable")


if __name__ == "__main__":
    main()
