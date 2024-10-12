from setuptools import setup, find_packages

setup(
    name='myapp',                       # パッケージ名（pip listで表示される）
    version="0.0.1",                    # バージョン
    description="this is description",  # 説明
    author='haneya',                    # 作者名
    packages=find_packages(),           # 使うモジュール一覧を指定する
    license='MIT'                       # ライセンス
)