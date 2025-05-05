import os
import configparser
import time
import json
import random
import datetime
from multiprocessing import Process
from kafka import KafkaProducer


config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.ini"), encoding='utf-8')

SERVERS = '{}:{}'.format(config_ini['DEFAULT']['HOST_IP'],'9092')

def test(name):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Kafka Producer is running...")

    while True:
        print(name,"send :" ,str(datetime.datetime.now()))

        data  = {
            "name": "Sample Entity",
            "temperature": random.randint(1, 10),
            "humidity": random.randint(1, 10)
        }
        print(json.dumps(data, indent=4))
        result = producer.send("source", data)
        producer.flush()
        time.sleep(3)

def main():
    t1 = Process(target=test, args=("Producer",))
    t1.start()

if __name__ == '__main__':
    main()