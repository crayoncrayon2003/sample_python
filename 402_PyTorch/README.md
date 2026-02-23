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


## 1.3. PyTorch のインストール
### Step 1: 
PyTorch 公式サイトにアクセスする。
+ https://pytorch.org/get-started/locally/

自身の CUDA Version に合致する PyTorch を選択する。

各自の環境に合わせて選択すること。

例えば、CUDA Toolkit 13.0.0 の場合、各項目を次のように選択する。

+ PyTorch Build: Stable (2.10.0)
+ Your OS: Linux
+ Package: Pip
+ Language: Python
+ Compute Platform: CUDA 13.0
+ Run this Command: 

これらを選択すると、環境に合わせたコマンドが表示される。

例えば、

```bash
(env) pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu130                       # For GPU users ( Stable )
```

ただし、RTX 5060 Ti など Blackwell アーキテクチャの GPU は、安定版 PyTorch が未対応である。

こののため nightly ビルドを使用する。

+ PyTorch Build: **Preview (Nightly)**
+ Your OS: Linux
+ Package: Pip
+ Language: Python
+ Compute Platform: **CUDA 12.8**
+ Run this Command: 

例えば、

```bash
(env) pip3 install --pre torch torchvision torchaudio --index-url https://download.pytorch.org/whl/nightly/cu128        # For GPU users ( Blackwell ex.RTX 5060 Ti )
```


### Step 2: 確認

```bash
(env) python3 -c "import torch; print(torch.__version__)"
(env) python3 -c "import torch; print('CUDA available:', torch.cuda.is_available())"
(env) jupyter kernelspec list
```
## 1.4. VS Code Extensions
* Python
* Jupyter

## 1.5. Deactivate
```bash
(env) deactivate
```

## 1.6. Remove
```bash
rm -rf env
```


# PyTorch とは

Facebook（Meta）が開発したディープラーニングフレームワークです。  
研究・LLM 分野で現在最も広く使われており、Hugging Face などのライブラリのバックエンドにもなっています。

---

## できること

### テンソル計算
NumPy に似た多次元配列（テンソル）を扱い、GPU で高速に演算できます。

### 自動微分（Autograd）
順伝播を実行するだけで、勾配（微分）を自動的に計算します。  
これがニューラルネットワークの学習（バックプロパゲーション）の核心です。

### ニューラルネットワーク構築（nn.Module）
層（Layer）を積み重ねてモデルを定義します。全結合層・CNN・RNN・Transformer など幅広く対応しています。

### カスタム学習ループ
`forward → loss計算 → backward → optimizer.step()` という学習の流れを自分で書くため、  
内部の動作を深く理解しながら柔軟にカスタマイズできます。

### GPU対応
`.to(device)` の一行で CPU / GPU を切り替えられます。大規模モデルの学習に必須です。

### カスタムデータセット（Dataset / DataLoader）
独自データを `Dataset` クラスで定義し、`DataLoader` でバッチ処理・シャッフル・並列読み込みを行います。

### 転移学習（Transfer Learning）
ImageNet などで事前学習済みのモデル（ResNet・VGGなど）を再利用して、  
少ないデータ・短い学習時間で高精度なモデルを作れます。


## 苦手なこと

- 古典的な機械学習（回帰・決定木など）には過剰（→ scikit-learn が適切）
- 学習ループを自分で書く必要があるため、コード量が多くなる
- 初学者には概念の習得コストがかかる

