import os
import configparser
import json
import uuid
from multiprocessing import Process
from kafka import KafkaConsumer, KafkaProducer

config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.ini"), encoding='utf-8')

SERVERS = '{}:{}'.format(config_ini['DEFAULT']['HOST_IP'],'9092')

def transform_data(source_data, schema):
    def process_field(field, source_data):
        if "source" in field:
            return source_data.get(field["source"], None)
        elif "default" in field:
            if field["default"] == "uuid":
                return str(uuid.uuid4())
            return field["default"]
        return None

    def process_record(schema_record, source_data):
        result = {}
        for field in schema_record["fields"]:
            if isinstance(field["type"], dict) and field["type"]["type"] == "record":
                result[field["name"]] = process_record(field["type"], source_data)
            else:
                result[field["name"]] = process_field(field, source_data)
        return result

    return process_record(schema, source_data)

def test(name):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"schema.json"), "r") as f:
        schema = json.load(f)

    consumer = KafkaConsumer(
        "source",
        bootstrap_servers=SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers=SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Kafka Streams is running...")

    for message in consumer:
        received_data = message.value
        print(name, "receive : ", json.dumps(received_data, indent=4))

        transformed_data = transform_data(received_data, schema)

        print(name, "send   : ", json.dumps(transformed_data, indent=4))
        producer.send("target", transformed_data)


def main():
    t1 = Process(target=test, args=("Streams ",))
    t1.start()

if __name__ == '__main__':
    main()