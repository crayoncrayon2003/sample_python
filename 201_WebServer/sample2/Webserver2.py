from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import base64

HOST = "0.0.0.0"
PORT = 8000
USERNAME = "csv_user"
PASSWORD = "csv_password"
CSV_FILENAME = "report.csv"
REALM = "CSV Download Area"

class HTTPHandler(BaseHTTPRequestHandler):

    def do_AUTHHEAD(self):
        """認証が必要な場合のレスポンス(401)を送信する"""
        self.send_response(401)
        self.send_header('WWW-Authenticate', f'Basic realm="{REALM}"')
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(b'Authentication required.')

    def do_GET(self):
        """GETリクエストを処理する"""
        # 認証情報のヘッダを取得
        auth_header = self.headers.get('Authorization')

        # 1. 認証ヘッダがない場合、認証を要求する
        if auth_header is None:
            self.do_AUTHHEAD()
            return

        # 2. 認証情報を解析し、ユーザー名とパスワードを検証する
        try:
            # ヘッダ形式 "Basic <base64文字列>" からエンコードされた部分を抽出
            encoded_credentials = auth_header.split(' ')[1]
            # Base64デコード
            decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
            # "ユーザー名:パスワード" の形式を分割
            username, password = decoded_credentials.split(':', 1)
        except (IndexError, ValueError):
            # ヘッダの形式が不正な場合も認証を要求
            self.do_AUTHHEAD()
            return

        # 3. 認証情報が正しいかチェック
        if username == USERNAME and password == PASSWORD:
            # 認証成功
            # 4. リクエストされたパスが正しいかチェック
            if self.path == f"/{CSV_FILENAME}":
                # メモリ上でCSVデータを動的に生成
                csv_data = "product_id,product_name,price,stock\n"
                csv_data += "PD001,Laptop,120000,50\n"
                csv_data += "PD002,Mouse,3500,200\n"
                csv_data += "PD003,Keyboard,7800,150\n"

                # 文字列をUTF-8のバイト列に変換
                response_body = csv_data.encode('utf-8')

                # 成功のレスポンス(200)を送信
                self.send_response(200)

                # レスポンスヘッダを送信
                self.send_header('Content-type', 'text/csv; charset=utf-8')
                self.send_header('Content-Length', str(len(response_body)))
                # ブラウザにダウンロード時のファイル名を提案するヘッダ
                self.send_header('Content-Disposition', f'attachment; filename="{CSV_FILENAME}"')
                self.end_headers()

                # レスポンスボディ（CSVデータ本体）を送信
                self.wfile.write(response_body)
            else:
                # パスが違う場合は 404 Not Found エラーを返す
                self.send_error(404, f"Not Found: Please access /{CSV_FILENAME}")
        else:
            # 認証失敗
            self.do_AUTHHEAD()

# --- メインの実行部分 (元のコードの構造を維持) ---
if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), HTTPHandler)
    server.serve_forever()
