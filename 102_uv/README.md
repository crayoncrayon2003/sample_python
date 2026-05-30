# 1. install
## 1.1. install uv
```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 1.2. set path
```bash
$ source ~/.bashrc
```

## 1.3. confirm
```bash
$ uv --version
```

# 2. Create Project
```bash
$ uv init my_project_name
$ cd my_project_name
$ ls
```

```
my_project_name/
├── .python-version   # Pythonバージョン指定
├── pyproject.toml    # プロジェクト設定・依存関係
└── README.md
└── main.py
```

# 3. Create Virtual Environment

This command should be executed within the **my_project_name** folder.

```bash
my_project_name$ uv venv
```

# 4. Activate Virtual Environment
```bash
my_project_name$  source .venv/bin/activate
(my_project_name) $ uv add "numpy==1.26.0" pandas scikit-learn
(my_project_name) $ uv remove pandas
```

# 5. run
```bash
(my_project_name) $ uv run python main.py
```

# 6. Reproducing dependencies
## 6.1.  create "Lock File"

lockファイルを生成

```bash
(my_project_name) $ uv lock
```


## 6.2.  Reinstall from "Lock File"

lockファイルから再インストール

```bash
(my_project_name) $ uv sync
```

## 6.3.  Why UV
Differences in Version Control Methods

+ requirements.txt  ->  Manual management
+ uv.lock           ->  Automatic management via uv


## 6.4.  Conversion
### 6.4.1 requirements.txt -> uv.lock

Read the contents of `requirements.txt` and automatically generate `uv.lock`.

```bash
(my_project_name) $ uv add -r requirements.txt
```


### 6.4.2 uv.lock -> requirements.txt

Export the contents of uv.lock to requirements.txt format.

```bash
(my_project_name) $ uv pip compile pyproject.toml -o requirements.txt
```


# 7. List of frequently used commands
# uv よく使うコマンド早見表

| やりたいこと | コマンド |
|---|---|
| プロジェクト作成 | `uv init <名前>` |
| 仮想環境作成 | `uv venv` |
| パッケージ追加 | `uv add <パッケージ>` |
| パッケージ削除 | `uv remove <パッケージ>` |
| インストール済み一覧 | `uv pip list` |
| スクリプト実行 | `uv run python <ファイル>` |
| 環境の同期 | `uv sync` |
| Pythonバージョン管理 | `uv python install 3.12` |


# 8. Deactivate Virtual Environment
```bash
(my_project_name) $ deactivate
```

# 9. Remove Virtual Environment
```bash
my_project_name$ rm -rf .venv
```

# 10. Remove Project
```bash
my_project_name$ cd ..
$ rm -rf my_project_name
```