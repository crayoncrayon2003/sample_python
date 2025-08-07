import pluggy

# "my_app"という名前空間でフックを管理することを宣言します
hookspec = pluggy.HookspecMarker("my_app")

@hookspec
def add_greeting(messages: list):
    """
    挨拶メッセージをリストに追加するためのフックスペック。
    プラグインはこの関数を実装します。
    """
    pass