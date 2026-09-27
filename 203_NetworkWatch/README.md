# wireshark

## install

```
sudo apt install wireshark
sudo apt install tshark
```

Tabキーを使って、 Yes を選んで Enter を押下する

## authority

```
sudo chmod +x /usr/bin/dumpcap
sudo usermod -aG wireshark "$USER"
```

# pyshark

## install

```
$ pip install pyshark
```


# Run
## リアルタイムで通信を確認する
```
python NetworkWatch1.py
```

## 通信を保存してから確認する
```
tshark -i lo -f "tcp port 8181" -F pcap -w a.pcap
python NetworkWatch2.py
```