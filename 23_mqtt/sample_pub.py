import paho.mqtt.client as mqtt
from time import sleep

broker    = "localhost"
port      = 8000
keepalive = 60
topic = "test/topic"

# callback function on connect
def on_connect(client, userdata, flag, rc):
    print("Connected with result code " + str(rc))

# callback function on disconnect
def on_disconnect(client, userdata, flag, rc):
    if rc != 0:
        print("Unexpected disconnection.")

# callback function on publish
def on_publish(client, userdata, mid):
    print("publish: {0}".format(mid))

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    client.connect(broker, port, keepalive)

    # start
    client.loop_start()

    # send msg
    while True:
        line = input()
        if(line == "q" or line == "Q"): break
        msg   = line
        client.publish(topic,msg)
        sleep(3)

if __name__ == '__main__':
    main()