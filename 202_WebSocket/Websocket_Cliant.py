import websocket
import _thread
import time
import json

class Websocket_Client():
    def __init__(self, host, port):
        super(Websocket_Client, self).__init__()
        self.uri = "ws://{0}:{1}".format(host, port)
        self.client = websocket.WebSocketApp(self.uri,
                                             on_open=self.on_open,
                                             on_message=self.on_message,
                                             on_error=self.on_error,
                                             on_close=self.on_close)

    # websocketクライアント起動
    def run_forever(self):
        self.client.run_forever()

    # websocket接続時のコールバック関数
    def on_open(self, ws):
        print('connected websocket server')
        # 接続した時についでにメッセージ送信用スレッドを起動
        _thread.start_new_thread(self.run, ())

    # websocket切断時のコールバック関数
    def on_close(self, ws, close_status_code, close_msg):
        print('disconnected websocket server')

    # messageを受信時のコールバック関数
    def on_message(self, ws, message):
        print(message)

    # エラーが起こった時のコールバック関数
    def on_error(self, ws, error):
        print(error)

    # サーバに対するメッセージ送信用のスレッド
    def run(self, *args):
        while True:
            time.sleep(0.1)
            x = input("send data:")
            if(x=='1'): self.SendMsg1()
            elif(x=='2'): self.SendMsg2()
            elif(x=='q'): break

        self.client.close()
        exit()

    def SendMsg1(self):
        print("SendMsg1")
        data={
            'MessageType' : 1,
            'Payload':{
                'Id' : 1
            }
        }
        self.client.send(json.dumps(data))

    def SendMsg2(self):
        print("SendMsg2")
        data={
            'MessageType' : 2,
            'Payload':{
                'Id' : 2
            }
        }
        self.client.send(json.dumps(data))

def main():
    ws_client = Websocket_Client("172.28.164.85", 8181)
    ws_client.run_forever()

if __name__ == '__main__':
    main()