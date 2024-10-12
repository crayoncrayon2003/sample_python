import paho.mqtt.client as mqtt

broker    = "localhost"
port      = 8000
keepalive = 60
topic = "test/topic"

# callback function on connect
def on_connect(client, userdata, flag, rc):
    print("Connected with result code " + str(rc)) 
    client.subscribe(topic)    # setting subscribe topic
    #client.subscribe(topic,1) # setting subscribe topic withc QoS
    #client.subscribe('#')     # setting subscribe all topic

# callback function on disconnect
def on_disconnect(client, userdata, flag, rc):
    if  rc != 0:
        print("Unexpected disconnection.")

# callback function on message
def on_message(client, userdata, msg):
    print("Received message '" + str(msg.payload) + "' on topic '" + msg.topic + "' with QoS " + str(msg.qos))

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.connect(broker, port, keepalive)

    # start
    client.loop_forever()

if __name__ == '__main__':
    main()