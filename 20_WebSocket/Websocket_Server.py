from websocket_server import WebsocketServer
import logging
import json
import time
import threading

class Websocket_Server(threading.Thread):
    def __init__(self, host, port):
        super(Websocket_Server, self).__init__()
        self.server = WebsocketServer(host=host, port=port, loglevel=logging.DEBUG)

    # websocketクライアント接続時のコールバック関数
    def new_client(self, client, server):
        print("new client connected and was given id {}".format(client['id']))
        # 全クライアントにメッセージを送信
        self.server.send_message_to_all("hey all, a new client has joined us")

    # websocketクライアント切断時のコールバック関数
    def client_left(self, client, server):
        print("client({}) disconnected".format(client['id']))

    # websocketクライアントからのメッセージ受信時のコールバック関数
    def message_received(self, client, server, message):
        print("client({}) said: {}".format(client['id'], message))
        # 全クライアントにメッセージを送信
        self.server.send_message_to_all(message)

    # websocketサーバーを起動する
    def run(self):
        # websocketクライアント接続時のコールバック関数をセット
        self.server.set_fn_new_client(self.new_client)
        # websocketクライアント切断時のコールバック関数をセット
        self.server.set_fn_client_left(self.client_left)
        # Websocketクライアントからのメッセージ受信時のコールバック関数をセット
        self.server.set_fn_message_received(self.message_received)
        # サーバを非同期で起動
        self.server.run_forever(True)

        # ユーザのキーボード入力待ち
        while(True):
            x=input("send data:")
            if(x=='104'): self.SendMsg1()
            elif(x=='105'): self.SendMsg2()
            elif(x=='q'): exit()
            time.sleep(1)

    def SendMsg1(self):
        print("SendMsg1")
        data={
            'MessageType' : 104,
            'Payload':{
                'Id' : 1
            }
        }
        self.server.send_message_to_all(json.dumps(data))

    def SendMsg2(self):
        print("SendMsg2")
        data={
            'MessageType' : 105,
            'Payload':{
                'Id' : 2
            }
        }
        self.server.send_message_to_all(json.dumps(data))

def main():
    ws_server = Websocket_Server("172.28.164.85", 8181)
    ws_server.start()

if __name__ == '__main__':
    main()
