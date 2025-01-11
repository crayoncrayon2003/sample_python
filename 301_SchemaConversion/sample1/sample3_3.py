import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from apache_atlas.client.base_client import AtlasClient

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


# Schema transform
def schema_transform(df, target_schema):
    if not target_schema or not target_schema.fields:
        raise ValueError("Target schema is invalid or empty. Transformation cannot proceed.")

    def convert_row(row):
        return tuple(row[col.name] if col.name in row else None for col in target_schema.fields)

    return df.rdd.map(convert_row).toDF(target_schema)

def main():
    # Setting SparkConf
    conf = SparkConf() \
            .setAppName("AtlasSchemaTransformationExample") \
            .set("spark.pyspark.driver.python", "/usr/bin/python3.12") \
            .set("spark.pyspark.python", "/usr/bin/python3.12")
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.12"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.12"
    spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .getOrCreate()

    # Define Atlas client
    atlas_client = AtlasClient("http://localhost:21000", ("admin", "admin"))

    # Load schemas from Apache Atlas
    schema_source = get_entities_from_atlas(atlas_client, "source_schema_sparktable", "default.source_schema_sparktable@spark")
    schema_target = get_entities_from_atlas(atlas_client, "target_schema_sparktable", "default.target_schema_sparktable@spark")

    if not schema_source or not schema_source.fields:
        print("Source schema is invalid or empty. Exiting.")
        return

    if not schema_target or not schema_target.fields:
        print("Target schema is invalid or empty. Exiting.")
        return

    # Show loaded schemas
    show_schema(schema_source, "Loaded SOURCE_SCHEMA")
    show_schema(schema_target, "Loaded TARGET_SCHEMA")

    # Create sample data
    data = [
        {"Name": "Alice", "Age": 25},
        {"Name": "Bob", "Age": 30},
        {"Name": "Cathy", "Age": 28}
    ]

    # Create DataFrame
    df_source = spark.createDataFrame(data, schema_source)

    # Show DataFrame
    df_source.show()

    # Transform to target schema from source schema
    df_target = schema_transform(df_source, schema_target)

    # Show transformed DataFrame
    df_target.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
