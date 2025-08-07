import pluggy

# "my_app"という名前空間のフックを実装することを宣言します
hookimpl = pluggy.HookimplMarker("my_app")

class JapaneseGreetingPlugin:
    @hookimpl
    def add_greeting(self, messages: list):
        messages.append("日本語プラグインからのこんにちは！")