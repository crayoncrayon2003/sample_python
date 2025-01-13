import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from flask import Flask, request, jsonify
from threading import Thread
from apache_atlas.client.base_client import AtlasClient

# Spark Config
CONF = SparkConf() \
    .setAppName("AtlasSchemaStreamingApp") \
    .set("spark.pyspark.python", "/usr/bin/python3") \
    .set("spark.pyspark.driver.python", "/usr/bin/python3")
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.12"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.12"
SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()

# Flask Server
APP = Flask(__name__)

# Atlas client
ATLAS_CLIENT = AtlasClient("http://localhost:21000", ("admin", "admin"))

def spark_type_to_atlas_type(spark_type):
    """Convert Spark DataType to Atlas-compatible string type."""
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

# schema transform
def schema_transform(df, target_schema):
    def convert_row(row):
        return (row["Name"], str(row["Age"]))
    return df.rdd.map(convert_row).toDF(target_schema)


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

        # create DataFrame
        df_source = SPARK.createDataFrame(input_data, schema_source)

        # Transform to target schema from source schema
        df_target = schema_transform(df_source, schema_target)

        # DataFrame to JSON
        transformed_data = df_target.toJSON().collect()

        # respons body
        output_json = {
            "table_source": input_table_source,
            "schema_source": input_schema_source_name,
            "table_target": input_table_target,
            "schema_target": input_schema_target_name,
            "data": [eval(record) for record in transformed_data]
        }

        return jsonify(output_json), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# run Flask Server
def run_flask():
    APP.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", 5000)))


if __name__ == "__main__":
    Thread(target=run_flask).start()
