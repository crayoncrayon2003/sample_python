# install

```
$ pip3 install paho-mqtt
```

# run

```
$ mkdir -p mosquitto/data/ mosquitto/log/
$ docker compose up -d
$ python mqtt_pub.py
$ python mqtt_sub.py
```
