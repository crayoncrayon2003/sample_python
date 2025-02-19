import os
import configparser
import json
from kafka import KafkaConsumer
from multiprocessing import Process

config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.ini"), encoding='utf-8')

SERVERS = '{}:{}'.format(config_ini['DEFAULT']['HOST_IP'],'9092')
#SERVERS = 'localhost:9092'

def test(name):
    consumer = KafkaConsumer(
        "target",
        bootstrap_servers=[SERVERS],
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Kafka Consumer is running...")

    for message in consumer:
        received_data = message.value
        print(name, "receive : ", json.dumps(received_data, indent=4))

def main():
    t1 = Process(target=test, args=("Consumer",))
    t1.start()

if __name__ == '__main__':
    main()