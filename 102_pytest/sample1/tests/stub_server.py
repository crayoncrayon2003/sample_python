from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import time
import json

class HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print("this is GET")

        # main pro
        time.sleep(1)

        # response
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        body = {
            "temperature": 10,
            "humidity"   : 20
        }
        print(body)
        self.wfile.write( json.dumps(body).encode('utf-8') )

if __name__ == "__main__":
    HOST = 'localhost'
    PORT = 8080
    print('http://{}:{}'.format(HOST, PORT))

    server = HTTPServer((HOST, PORT), HTTPHandler)
    server.serve_forever()
