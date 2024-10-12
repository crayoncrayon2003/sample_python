from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import time

def showRequest(self):
    parsed_path = urlparse(self.path)
    print('path   = {}'.format(parsed_path.path))
    print('query  = {}'.format(parse_qs(parsed_path.query)))
    print('header = {}'.format(self.headers))

class HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print("this is GET")

        # request
        showRequest(self)

        # main pro
        time.sleep(1)

        # response
        status = 200
        self.send_response(status)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        body = "Hello world"
        self.wfile.write( body.encode(encoding='utf-8') )

    def do_POST(self):
        print("this is POST")

        # request
        showRequest(self)

        # main pro
        print('body   = {}'.format(self.rfile.read(int(self.headers['content-length'])).decode('utf-8')))

        # response
        self.send_response(200)
        self.end_headers()

    def do_PUT(self):
        print("this is PUT")

        # main pro
        showRequest(self)

        # 本体処理
        print('body   = {}'.format(self.rfile.read(int(self.headers['content-length'])).decode('utf-8')))

        # response
        self.send_response(200)
        self.end_headers()

    def do_PATCH(self):
        print("this is PATCH")

        # request
        showRequest(self)

        # main pro
        print('body   = {}'.format(self.rfile.read(int(self.headers['content-length'])).decode('utf-8')))

        # response
        self.send_response(200)
        self.end_headers()

    def do_DELETE(self):
        print("this is DELETE")

        # request
        showRequest(self)

        # main pro
        time.sleep(1)

        # response
        self.send_response(400)
        self.end_headers()

if __name__ == "__main__":
    HOST = '192.168.0.26'
    PORT = 8080
    print('http://{}:{}'.format(HOST, PORT))

    server = HTTPServer((HOST, PORT), HTTPHandler)
    server.serve_forever()
