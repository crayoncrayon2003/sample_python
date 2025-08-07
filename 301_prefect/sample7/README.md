# install
```
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

# uninstall
```
deactivate
rm -rf env
```

# directory structure
etl-framework/
├── scripts/                        # 共通フレームワーク
│   ├── __init__.py
│   ├── core/                       # フレームワーク本体
│   │   ├── __init__.py
│   │   ├── pipeline/               # パイプライン定義・実行エンジン
│   │   │   ├── __init__.py
│   │   │   └── step_executor.py    # ステップ実行管理
│   │   ├── plugin_manager/         # プラグイン管理システム
│   │   │   ├── __init__.py
│   │   │   ├── hooks.py            # プラグインが実装するフックの仕様
│   │   │   └── manager.py          # プラグイン管理メインクラス
│   │   ├── data_container/         # データコンテナ定義
│   │   │   ├── __init__.py
│   │   │   ├── container.py        # DataContainerクラス
│   │   │   └── formats.py          # サポートフォーマット定義
│   │   └── config/                 # 設定管理
│   │       ├── __init__.py
│   │       ├── loader.py           # 設定ファイル読み込み
│   │       └── validator.py        # 設定バリデーション
│   │
│   ├── plugins/                    # プラグイン群
│   │   ├── __init__.py
│   │   ├── extractors/             # データ取得プラグイン
│   │   │   ├── __init__.py
│   │   │   ├── from_local_file.py  # ローカルファイル取得
│   │   │   ├── from_local_json.py  # ローカルJSON取得
│   │   │   ├── from_http.py        # HTTP API取得
│   │   │   ├── from_ftp.py         # FTP取得
│   │   │   ├── from_scp.py         # SCP取得
│   │   │   └── from_database.py    # データベース取得
│   │   ├── cleansing/              # データクレンジングプラグイン
│   │   │   ├── __init__.py
│   │   │   ├── archive_extractor.py # ZIP/圧縮ファイル展開
│   │   │   ├── encoding_converter.py # 文字コード変換
│   │   │   ├── format_detector.py  # ファイル形式自動判定
│   │   │   ├── duplicate_remover.py # 重複データ除去
│   │   │   └── null_handler.py     # NULL値処理
│   │   ├── transformers/           # データ変換プラグイン
│   │   │   ├── __init__.py
│   │   │   ├── with_duckdb.py      # DuckDB変換
│   │   │   ├── with_jinja2.py      # Jinja2変換
│   │   │   ├── to_ngsi.py          # NGSI形式変換
│   │   │   ├── csv_processor.py    # CSV処理
│   │   │   ├── dataframe_joiner.py # DF処理
│   │   │   ├── json_processor.py   # JSON処理
│   │   │   ├── gtfs_processor.py   # GTFS処理
│   │   │   └── shapefile_processor.py # シェープファイル処理
│   │   ├── validators/             # バリデーションプラグイン
│   │   │   ├── __init__.py
│   │   │   ├── json_schema.py      # JSONスキーマ検証
│   │   │   ├── data_quality.py     # データ品質チェック
│   │   │   ├── ngsi_validator.py   # NGSI形式検証
│   │   │   └── business_rules.py   # ビジネスルール検証
│   │   └── loaders/                # データ登録プラグイン
│   │       ├── __init__.py
│   │       ├── to_local_file.py    # ローカルファイル出力
│   │       ├── to_http.py          # HTTP POST/PUT
│   │       ├── to_ftp.py           # FTP アップロード
│   │       ├── to_scp.py           # SCP アップロード
│   │       ├── to_context_broker.py # Context Broker登録
│   │       └── to_database.py      # データベース登録
│   │
│   └── utils/                      # ユーティリティ
│       ├── __init__.py
│       ├── config_loader.py        # 設定読み込み
│       ├── sql_template.py         # SQLテンプレート処理
│       ├── file_utils.py           # ファイル操作ユーティリティ
│       └── logger.py               # ログ設定
│
├── ETL1/                           # CSV to Parquet and Report
│   ├── 00_flows/                   # フロー定義
│   │   ├── .gitkeep
│   │   └── etl_flow.py             # フロー
│   ├── 01_data/                    # データディレクトリ
│   │   ├── input/
│   │   │   ├── .gitkeep
│   │   │   └── source_data.csv
│   │   ├── working/
│   │   │   └── .gitkeep
│   │   └── output/
│   │       └── .gitkeep
│   ├── 02_cleansing/               # クレンジング用スキーマ・設定
│   │   ├── .gitkeep
│   │   ├── rules.yml               # クレンジングルール設定
│   │   └── schemas/                # クレンジング前後のスキーマ
│   │       ├── raw_data_schema.json
│   │       └── cleansed_data_schema.json
│   ├── 03_validation/              # バリデーション用スキーマ・設定
│   │   ├── .gitkeep
│   │   ├── rules.yml               # バリデーションルール設定
│   │   └── schemas/                # バリデーション用スキーマ
│   │       ├── input_schema.json   # スキーマファイル
│   │       └── output_schema.json  # スキーマファイル
│   ├── 04_templates/               # Jinja2テンプレート
│   │   ├── .gitkeep
│   │   └── report.html.j2          # テンプレート
│   ├── 05_queries/                 # DuckDB SQLクエリ
│   │   ├── .gitkeep
│   │   └── add_timestamp.sql       # クエリ
│
├── ETL2/                           # CSV to NGSI-v2 JSON
│   ├── 00_flows/
│   │   ├── .gitkeep
│   │   └── etl_flow.py
│   ├── 01_data/
│   │   ├── input/
│   │   │   ├── .gitkeep
│   │   │   └── measurements.csv
│   │   ├── working/
│   │   │   └── .gitkeep
│   │   └── output/
│   │       └── .gitkeep
│   ├── 02_cleansing/               # クレンジング用スキーマ・設定
│   │   ├── .gitkeep
│   │   ├── rules.yml               # クレンジングルール設定
│   │   └── schemas/                # クレンジング前後のスキーマ
│   │       ├── raw_data_schema.json
│   │       └── cleansed_data_schema.json
│   ├── 03_validation/              # バリデーション用スキーマ・設定
│   │   ├── .gitkeep
│   │   ├── rules.yml               # バリデーションルール設定
│   │   └── schemas/                # バリデーション用スキーマ
│   │       ├── input_schema.json
│   │       └── output_schema.json
│   ├── 04_templates/               # Jinja2テンプレート
│   │   ├── .gitkeep
│   │   └── to_ngsiv2.json.j2
│   ├── 05_queries/                 # DuckDB SQLクエリ
│   │   ├── .gitkeep
│   │   ├── 01_validate_source.sql
│   │   └── 02_structure_to_ngsi.sql
│   └── config.yml


# run
```
python ETL1/00_flows/etl_flow.py
```
