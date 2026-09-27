# pytest sample1

以下はリポジトリのルートから開始する手順です（bash）。
各 Case のコマンドは `103_pytest/sample1` で実行します。

## 仮想環境の作成と依存パッケージのインストール

```bash
cd 103_pytest/sample1
python -m venv env
source env/bin/activate
python -m pip install --upgrade pip setuptools
python -m pip install -r requirements.txt
```

コードスタイルの検査には `pytest-flake8` ではなく `flake8` を直接使います。
既存の環境に `pytest-flake8` が残っていても、`pytest.ini` でそのプラグインを
無効化するため、pytest の起動時に互換性エラーが発生するのを防ぎます。

## テストの実行

### Case1: 通常の実行

```bash
python -m pytest
```

### Case2: 並列実行

```bash
python -m pytest -n 2
```

### Case3: コードスタイルの検査（PEP 8）

```bash
python -m flake8 src tests
```

### Case4: HTML レポートの出力

```bash
python -m pytest --html=report.html --self-contained-html
```

### Case5: カバレッジの計測

```bash
python -m pytest --cov=src.main --cov-report=term-missing
```

### Case6: コードの複雑度の解析

```bash
lizard src/main.py
```

`tests/` から `python -m pytest` を実行することもできます。
ゼロ除算のテストでは、Python が送出する `ZeroDivisionError` を検証します。

## スタブサーバーの手動起動（任意）

`test_get` は HTTP 通信をモックするため、テスト時のサーバー起動は不要です。
実際の HTTP 通信を試す場合だけ、別ターミナルで仮想環境を有効にし、
`103_pytest/sample1` から次を実行します。

```bash
python tests/stub_server.py
```

終了するには `Ctrl+C` を押します。

## 仮想環境の終了・削除

```bash
deactivate
# 仮想環境が不要になった場合のみ実行
rm -rf env
```
