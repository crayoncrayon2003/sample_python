# install

```
python -m pip install -r requirements.txt
```

`import websocket` に対応するクライアントパッケージは `websocket-client` です。
`websocket` は別のパッケージで、`WebSocketApp` を提供しません。

以前の手順で `websocket` をインストールした場合は、同じ仮想環境で次を実行してください。
両パッケージは同じ `websocket` モジュール名を使うため、削除後に
`websocket-client` を再インストールします。

```bash
python -m pip uninstall -y websocket
python -m pip install --force-reinstall websocket-client
python -m pip install -r requirements.txt
python -c "import websocket; print(websocket.WebSocketApp)"
```

# run

`202_WebSocket` ディレクトリでサーバーを起動します。

```bash
python Websocket_Server.py
```

別のターミナルで同じ仮想環境を有効にし、クライアントを起動します。

```bash
python Websocket_Cliant.py
```

数字の**1**または**2**を入力します