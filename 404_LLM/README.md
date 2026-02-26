# LLM とは

**LLM（Large Language Model / 大規模言語モデル）** は、大量のテキストデータで学習した深層学習モデルです。  
文章の生成・要約・翻訳・質問応答・コード生成など、幅広いタスクをこなせます。

---

## LLM の種類

### クラウド API 型（ローカル不要）
自分の PC にモデルを置かず、API 経由でクラウド上の LLM を利用します。

| サービス | 代表モデル | 特徴 |
|----------|-----------|------|
| OpenAI | GPT-4o・GPT-4 | 最も広く使われる |
| **Anthropic** | **Claude 3.5 / 3.7** | **後述** |
| Google | Gemini 1.5 / 2.0 | マルチモーダルが強い |
| Mistral AI | Mistral Large | 軽量・高性能 |

### ローカル実行型（自分の PC で動かす）
モデルをローカルにダウンロードして実行します。API コストが不要で、プライバシーも確保できます。

| ツール | 特徴 |
|--------|------|
| **Ollama** | コマンド一発で動く。最もシンプル。**後述** |
| llama.cpp | C++ベース。最も軽量・高速 |
| **llama-cpp-python** | llama.cpp の Python バインディング。**後述** |
| LM Studio | GUI で手軽にローカル LLM を動かせる |
| vLLM | 高速推論サーバ。本番環境向け |

### オープンモデル（weights が公開されている）
誰でも無料でダウンロードして使えるモデルです。

| モデル | 開発元 | 特徴 |
|--------|--------|------|
| **LLaMA 3** | Meta | 最も広く使われるオープンモデル。**後述** |
| Mistral / Mixtral | Mistral AI | 軽量・高性能 |
| Gemma 2 | Google | 小型で高性能 |
| Phi-4 | Microsoft | 小型モデルとして優秀 |
| Qwen 2.5 | Alibaba | 多言語対応が強い |
| DeepSeek | DeepSeek | コード生成が得意 |

---

## 本サンプルでの選択

複数の選択肢の中から、以下の理由で選択しています。

| 用途 | 選択 | 理由 |
|------|------|------|
| ローカル LLM 実行 | **Ollama（Docker）** | WSL 環境を汚さず Docker コンテナで動かせる |
| Python から LLM 制御 | **llama-cpp-python** | Python から直接モデルを制御できる |
| オープンモデル | **LLaMA 3** | 最も情報が多く、日本語にも対応 |
| 事前学習済みモデル活用 | **Hugging Face Transformers** | 業界標準。モデルの種類が最も豊富 |
| RAG・ベクトルDB | **FAISS** | Meta 製。高速・軽量でローカル動作 |
| LLM アプリ開発 | **LangChain** | 最も普及しており情報が多い |
| クラウド API | **Anthropic（Claude）** | 高い安全性と性能。MCP との親和性が高い |

---

## サンプルのロードマップ

```
【基盤】LLM を理解する
  ├── Ollama（Docker）でローカル LLM を動かす
  └── llama-cpp-python で Python から制御する
        ↓
【活用】Hugging Face Transformers
  ├── 事前学習済みモデルの利用
  └── Fine-tuning（LoRA）
        ↓
【応用】RAG + ベクトルDB（FAISS）
  ├── ドキュメントの埋め込み
  └── 検索拡張生成
        ↓
【開発】LangChain
  ├── チェーン構築
  └── エージェント
        ↓
【接続】LLM API（Anthropic / Claude）
        ↓
【統合】MCP サーバ / FastMCP  ← ゴール
```

---

## できること

### テキスト生成
プロンプトに対して自然な文章を生成します。

### 要約・翻訳
長い文章を短くまとめたり、別の言語に翻訳します。

### 質問応答（QA）
ドキュメントや知識ベースを参照して質問に答えます。

### コード生成
自然言語の指示からコードを生成・修正します。

### RAG（検索拡張生成）
独自ドキュメントをベクトルDBに格納し、LLM の回答精度を向上させます。

### Fine-tuning
独自データでモデルを追加学習させ、特定タスクに特化させます。

---

## 苦手なこと

- 学習データのカットオフ以降の最新情報は知らない（→ RAG で補完）
- 数値計算・論理推論は苦手なことがある
- ハルシネーション（もっともらしい嘘）を生成することがある
- 大きなモデルは GPU メモリを大量に消費する

---

## サンプル一覧

| ファイル | ステップ | テーマ |
|----------|----------|--------|
| sample1.ipynb | 基盤 | Ollama（Docker）でローカル LLM を動かす |
| sample2.ipynb | 基盤 | llama-cpp-python で Python から制御 |
| sample3.ipynb | 活用 | Hugging Face Transformers 基礎 |
| sample4.ipynb | 活用 | Fine-tuning（LoRA） |
| sample5.ipynb | 応用 | RAG + FAISS |
| sample6.ipynb | 開発 | LangChain 基礎 |
| sample7.ipynb | 接続 | Anthropic API（Claude） |

---

# 1. Install

## 1.0. PC（ハードウェア）に、GPU (CUDA) 有無を確認する
```bash
nvidia-smi
```

- 出力表示あり         → PCにGPUあり
- `command not found` → PCにGPUなし。CPU でも動作する

---

## 1.1. Docker のインストール

### Step 1: Docker Engine のインストール

省略

### Step 2: NVIDIA Container Toolkit のインストール

WSL で Docker から GPU を使うには NVIDIA Container Toolkit のインストールが必要です。

```bash
# NVIDIA Container Toolkit のインストール
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | 
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

# Docker に設定を反映
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```

### Step 3: NVIDIA Container Toolkit のインストール結果確認

```bash
# バージョン確認
nvidia-ctk --version

# パッケージとして入っているか確認
dpkg -l | grep nvidia-container-toolkit

# Docker が GPU を認識しているか確認
docker run --rm --gpus all nvidia/cuda:12.0-base-ubuntu22.04 nvidia-smi
```

---

## 1.2. Ollama を Docker で起動

### Step 1: Ollama コンテナの起動

GPU あり（RTX 5060 Ti など）の場合
```bash
docker run -d --gpus=all -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
```

GPU なし（CPU のみ）の場合
```bash
docker run -d -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
```

### Step 2: LLaMA 3 のダウンロード（約4GB）
```bash
docker exec -it ollama ollama pull llama3       # Meta製・最も広く使われるオープンモデル
docker exec -it ollama ollama pull gemma2:9b    # Google製・日本語対応
```

### Step 3: 動作確認
```bash
# モデル一覧
docker exec -it ollama ollama list

# テスト実行
docker exec -it ollama ollama run llama3 "日本語で自己紹介してください"
docker exec -it ollama ollama run gemma2:9b "日本語で自己紹介してください"
```

### コンテナの管理
```bash
docker stop ollama       # 停止（コンテナollamaを停止する。）
docker start ollama      # 起動（コンテナollamaを起動する。）
docker rm ollama         # 削除（コンテナollamaを削除する。モデルデータは ollama ボリュームに残る。）
docker volume rm ollama  # 削除（ollama ボリュームを削除する。）
```

---

## 1.3. 仮想環境の作成
```bash
python3 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install numpy pandas matplotlib
pip install jupyter ipykernel
python3 -m ipykernel install --user --name=env --display-name "Python (env)"
```

---

## 1.4. Python ライブラリのインストール

### Ollama Python クライアント
```bash
pip install ollama
```

### llama-cpp-python

GPU版・CUDA対応

```bash
CMAKE_ARGS="-DGGML_CUDA=on" pip install llama-cpp-python --force-reinstall
```

CPU版
```bash
pip install llama-cpp-python
```

### Hugging Face Transformers
```bash
pip install transformers accelerate peft datasets
```

### RAG・ベクトルDB
```bash
pip install faiss-cpu sentence-transformers
```

### LangChain
```bash
pip install langchain langchain-community langchain-huggingface
```

### Anthropic API
```bash
pip install anthropic
```

---

## 1.5. 確認
```bash
# Ollama（Docker）
docker exec -it ollama ollama list

# Python ライブラリ
python3 -c "import ollama; print('ollama OK')"
python3 -c "import transformers; print(transformers.__version__)"
python3 -c "import langchain; print(langchain.__version__)"
python3 -c "import anthropic; print('anthropic OK')"
python3 -c "import faiss; print('faiss OK')"
```

---

## 1.6. Anthropic API キーの設定
```bash
vim ~/.bashrc
```

末尾に以下を追加する

```bash
export ANTHROPIC_API_KEY="your-api-key-here"
```

保存して有効化する

```bash
source ~/.bashrc
```

---

## 1.7. VS Code Extensions
* Python
* Jupyter

---

## 1.8. Deactivate
```bash
deactivate
```

## 1.9. Remove
```bash
# 仮想環境の削除
rm -rf env

# Hugging Face キャッシュの削除
rm -rf ~/.cache/huggingface
rm -rf .cache/huggingface

# Docker の削除
docker stop ollama       # 停止（コンテナ ollama を停止する。）
docker rm ollama         # 削除（コンテナ ollama を削除する。モデルデータは ollama ボリュームに残る。）
docker volume rm ollama  # 削除（ollama ボリュームを削除する。）
```

