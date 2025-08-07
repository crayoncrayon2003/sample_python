import pluggy
import hookspecs
import os
import importlib.util
import inspect
from typing import List, Any

def register_plugins(pm: pluggy.PluginManager, plugins_dir: str):
    for filename in os.listdir(plugins_dir):
        # .pyで終わり、__init__.pyでないファイルを対象
        if filename.endswith(".py") and filename != "__init__.py":
            module_path = os.path.join(plugins_dir, filename)
            module_name = f"{plugins_dir}.{filename[:-3]}"

            # モジュールを読み込む
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # モジュール内のクラスを探索してプラグインとして登録
                for _, obj in inspect.getmembers(module):
                    if inspect.isclass(obj):
                        # issubclass(obj, object)などを追加してより厳密にすることも可能
                        pm.register(obj())
                        print(f"プラグインを登録しました: {obj.__name__}")


def call_specific_plugin(pm: pluggy.PluginManager, class_name: str, messages: List[str]):
    registered_plugins: List[Any] = pm.get_plugins()

    # クラス名で目的のプラグインを探す
    for plugin in registered_plugins:
        if plugin.__class__.__name__ == class_name:
            # プラグインのインスタンスのメソッドを直接呼び出す
            plugin.add_greeting(messages)
            return # 見つかったら処理を終了

    print(f"\nプラグイン '{class_name}' は見つかりませんでした。")

def main():
    print("1. プラグインマネージャーの初期化")
    pm = pluggy.PluginManager("my_app")

    print("2. フック仕様の登録")
    pm.add_hookspecs(hookspecs)

    print("3. プラグインの登録")
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugins")
    register_plugins(pm, path)

    print("4. フックの呼び出し")
    initial_messages = []
    pm.hook.add_greeting(messages=initial_messages)

    print("5. 結果の表示")
    for msg in initial_messages:
        print(f"- {msg}")


    print("5. EnglishGreetingPluginの呼び出し")
    english_message = []
    call_specific_plugin(pm, "EnglishGreetingPlugin", english_message)

    print("7. 結果の表示")
    print(f"- {english_message[0]}")


if __name__ == "__main__":
    main()