import pluggy

# "my_app"という名前空間のフックを実装することを宣言します
hookimpl = pluggy.HookimplMarker("my_app")

class EnglishGreetingPlugin:
    @hookimpl
    def add_greeting(self, messages: list):
        messages.append("Hello from the English Plugin!")