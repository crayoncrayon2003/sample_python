# 1. Install

## 1.0. PC（ハードウェア）に、GPU (CUDA) 有無を確認する
```bash
nvidia-smi
```

- 出力表示あり         → PCにGPUあり。1.1. CUDAのインストール
- `command not found` → PCにGPUなし。CPU版をインストールする


## 1.1. CUDAのインストール
### Step 1: CUDA バージョンの確認
```bash
nvidia-smi
```

右上に `CUDA Version: 13.x` のような表示がある。これがGPUのサポートする最大バージョンとなる。

ここでは、例に、CUDA Version: 13.0 を用いる。


### Step 2: CUDA Toolkit のインストール
CUDA 公式サイトにアクセスする。
+ https://developer.nvidia.com/cuda-toolkit-archive

自身の CUDA Version に合致するCUDA Toolkitを選択する。

各自の環境に合わせて選択すること。

例えば、CUDA Toolkit 13.0.0 かつ　Ubuntu on WSLの場合、各項目を次のように選択する。

+ Operating System: Linux
+ Architecture: x86_64
+ Distribution: WSL-Ubuntu
+ Version: 2.0
+ Installer Type: deb (network) 

これらを選択すると、環境に合わせたコマンドが表示される。

例 1 :

```bash
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get -y install cuda-toolkit-13-0 # for CUDA 13.0
```

例 2 :
```bash
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get -y install cuda-toolkit-12-8 # for CUDA 12.8
```

### Step 3: 環境変数の設定
vim で開く（sudo なし）

```bash
vim ~/.bashrc
```

末尾に以下を入力する

```bash
export PATH=/usr/local/cuda/bin:$PATH
export LD_LIBRARY_PATH=/usr/lib/wsl/lib:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:$LD_LIBRARY_PATH # for CUDA 13
#export LD_LIBRARY_PATH=/usr/lib/wsl/lib:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:$LD_LIBRARY_PATH # for CUDA 12
```

保存して有効化する

```bash
source ~/.bashrc
```

### Step 4: CUDA の確認

```bash
nvcc --version
```

**シンボリックの切り替え**

cuda 12.8 を使う場合

```bash
sudo ln -sfn /usr/local/cuda-12.8 /usr/local/cuda
nvcc --version  # 12.8 になっているか確認
```

cuda 13.0 を使う場合

```bash
sudo ln -sfn /usr/local/cuda-13.0 /usr/local/cuda
nvcc --version  # 13.0 になっているか確認
```

## 1.2. 仮想環境の作成
```bash
python3 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) pip install numpy pandas matplotlib scikit-learn
(env) pip install jupyter ipykernel
(env) python3 -m ipykernel install --user --name=env --display-name "Python (env)"
```

## 1.3.NVIDIA cuDNN のインストール
CUDA 公式サイトにアクセスする。
+ https://developer.nvidia.com/cudnn-downloads

cuDNNは、最新バージョンをインストールする

+ Operating System: Linux
+ Architecture: x86_64
+ Distribution: Ubuntu
+ Version: 24.04
+ Installer Type: deb (local) 
+ Configuration: FULL

```bash
wget https://developer.download.nvidia.com/compute/cudnn/9.19.0/local_installers/cudnn-local-repo-ubuntu2404-9.19.0_1.0-1_amd64.deb
sudo dpkg -i cudnn-local-repo-ubuntu2404-9.19.0_1.0-1_amd64.deb
sudo cp /var/cudnn-local-repo-ubuntu2404-9.19.0/cudnn-*-keyring.gpg /usr/share/keyrings/
sudo apt-get update
sudo apt-get -y install cudnn
```

```bash
sudo apt-get -y install cudnn9-cuda-13 # for CUDA 13
sudo apt-get -y install cudnn9-cuda-12 # for CUDA 12
```

## 1.4. TensorFlow のインストール
### Step 1:
TensorFlow 公式サイトにアクセスする。
+ https://www.tensorflow.org/install

自身の環境に合致する TensorFlow を選択する。

各自の環境に合わせて選択すること。

例えば、CUDA Toolkit 13.0.0 の場合、安定版をインストールする。

```bash
(env) pip install tensorflow[and-cuda] # For GPU users ( Stable )
(env) pip install tensorflow           # For GPU users ( Stable )
```

ただし、RTX 5060 Ti など Blackwell アーキテクチャの GPU は、安定版 TensorFlow が未対応である。

このため nightly ビルドを使用する。

```bash
(env) pip install tf-nightly            # For GPU users ( Blackwell ex.RTX 5060 Ti )
```

### Step 2: 確認

```bash
(env) python3 -c "import tensorflow as tf; print(tf.__version__)"
(env) python3 -c "import tensorflow as tf; print(tf.reduce_sum(tf.random.normal([1000, 1000])))"
(env) python3 -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
(env) jupyter kernelspec list
```

## 1.5. VS Code Extensions
* Python
* Jupyter

## 1.6. Deactivate
```bash
(env) deactivate
```

## 1.7. Remove
```bash
rm -rf env
```


# TensorFlow / Keras とは

Google が開発したディープラーニングフレームワークです。  
**Keras** は TensorFlow の高レベル API で、シンプルな記述でニューラルネットワークを構築できます。  
産業界での利用実績が豊富で、`model.fit()` 一行で学習が完結するのが大きな特徴です。


## できること

### ニューラルネットワーク構築（Keras Sequential / Functional API）
層を積み重ねるだけでモデルを定義できます。シンプルな Sequential API と、複雑なモデルに対応した Functional API の2種類があります。

### 学習の自動化（model.fit）
`model.compile()` で損失関数・オプティマイザを指定し、`model.fit()` を呼ぶだけで学習が完結します。PyTorch のように学習ループを自分で書く必要がありません。

### GPU対応
TensorFlow が自動的に GPU を検出して使用します。明示的なデバイス指定なしに GPU で学習できます。

### コールバック
学習中に Early Stopping・モデル保存・学習率スケジューリングなどを自動で行う仕組みです。

### 転移学習（Transfer Learning）
Keras Applications に ResNet・VGG・EfficientNet などの事前学習済みモデルが内蔵されており、数行で転移学習を実装できます。

### TensorBoard
学習の進捗・Loss・精度などをブラウザでリアルタイムに可視化できるツールです。

## PyTorch との比較

| | PyTorch | TensorFlow / Keras |
|---|---|---|
| 学習ループ | 自分で書く | `model.fit()` 一行 |
| 記述量 | 多い | 少ない |
| 柔軟性 | 高い | 中程度 |
| デバッグのしやすさ | しやすい | やや難しい |
| 研究での人気 | 高い | 中程度 |
| 産業利用実績 | 高い | 高い |
| GPU指定 | `.to(device)` | 自動 |


## 苦手なこと

- 古典的な機械学習（回帰・決定木など）には過剰（→ scikit-learn が適切）
- PyTorch に比べて内部動作がブラックボックスになりやすい
- 最新の研究実装は PyTorch ベースが多く、再現が難しいことがある

