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
(env) python3 -c "import torch; print(torch.cuda.get_device_name(0))"
(env) jupyter kernelspec list
```

## 1.4. Yoloインストール
```bash
(env) pip install ultralytics
(env) pip install opencv-python
```

## 1.6. VS Code Extensions
* Python
* Jupyter

## 1.7. Deactivate
```bash
(env) deactivate
```

## 1.8. Remove
```bash
rm -rf env
```

# 2. Ubuntu on WSLにカメラを認識させる

Ubuntu on WSLにカメラを認識させる方法はある。

一方、カメラの種類によっては、WSL側からカメラにアクセスできない。


+ 1. Windows側からカメラにアクセスして、ffmpegでMJPEGストリーム配信
+ 2. Ubuntu on WSLで 直接カメラにアクセスできるようデバイス認識

## 2.1. Windows側からカメラにアクセスして、ffmpegでMJPEGストリーム配信
### Step1: ffmpegをダウンロードする
以下にアクセスする。
+ https://www.gyan.dev/ffmpeg/builds/#release-builds

latest release から 「 ffmpeg-release-essentials.zip 」 を ダウンロードする。

### Step2: 任意フォルダにZIPファイルを展開する
例えば、
+ C:\ffmpeg-8.0.1-essentials_build\> 

以降は、binフォルダ内に移動して実施する
+ C:\ToolInstallFolder\ffmpeg-8.0.1-essentials_build\bin\> 

### Step3: カメラデバイス名を取得する
次のコマンドで、カメラ名称を取得する。

例えば、USB Camera だとする。

```powershell
.\ffmpeg.exe -list_devices true -f dshow -i dummy

[dshow @ 00000279f3533540] "EƖgato Virtual Camera" (video)
[dshow @ 00000279f3533540] "USB Camera" (video)
Error opening input file dummy.
```

### Step3: windows側でHTTTPサーバとして、カメラ映像をMJPEGストリーム配信
次の video= 部分でカメラ名称を指定する。

コマンドを実行するとストリーミング配信が始まる。

なお、Windows側とWSL側は、ポートフォワーディングをしている。UDPでストリーミング配信したいけど、NAT越え出来ないためTCPを使う。

```powershell
.\ffmpeg.exe -f dshow -rtbufsize 256M -i video="USB Camera" -vf scale=640:360 -c:v libx264 -preset ultrafast -tune zerolatency -f mpegts tcp://0.0.0.0:12345?listen=1
```

### Step4: 接続テスト
powershellで、WSL 側の IPドレス を確認する。

```powershell
ipconfig
```

pythonコード内のIPアドレスを書き換えてテストする

```bash
(env) python sample0_http.py
```


### Step3: windows側でHTTTPサーバとして、カメラ映像をMJPEGストリーム配信

## 2.2. Ubuntu on WSLで 直接カメラにアクセスできるようデバイス認識
### Step1: usbipdをインストール
PowerShellを管理者権限で開いて、usbipdをインストール

```powershell
winget install usbipd
```

### Step2: PowerShellを再起動
PowerShellを一旦閉じる。

再度、管理者権限でPowerShellを開く。

### Step3: usbipdのインストール結果の確認
usbipdのインストール結果を確認する。

```powershell
usbipd --version
```

### Step3: usbipdのインストール結果の確認
WSLに渡せるUSBデバイスの一覧を表示する

```powershell
usbipd list

> Connected:
> BUSID  VID:PID    DEVICE       STATE
> 4-2    XXXX:XXXX  USB Camera   Not shared
> 8-3    YYYY:YYYY  USB DEVICE   Not shared
> 8-4    ZZZZ:ZZZZ  USB DEVICE   Not shared
```

### Step4: Windows側からWSL側にカメラを渡す

Windows側でUSB機器を「共有可能状態」にする

```powershell
usbipd bind --busid 4-2
```

STATEが、Not sharedからsharedへ変化する。

```powershell
usbipd list

> Connected:
> BUSID  VID:PID    DEVICE       STATE
> 4-2    XXXX:XXXX  USB Camera   shared
> 8-3    YYYY:YYYY  USB DEVICE   Not shared
> 8-4    ZZZZ:ZZZZ  USB DEVICE   Not shared
```

「共有可能状態」のUSBを WSLに接続する

```powershell
usbipd attach --wsl --busid 4-2
```

STATEが、sharedからAttachedへ変化する。

エラーが発生する場合、Windowsがカメラを占有している。PC再起動などで占有状態を解除できる。

```powershell
usbipd list

> Connected:
> BUSID  VID:PID    DEVICE       STATE
> 4-2    XXXX:XXXX  USB Camera   shared
> 8-3    YYYY:YYYY  USB DEVICE   Not shared
> 8-4    ZZZZ:ZZZZ  USB DEVICE   Not shared
```

WSLから切断するには、デタッチ・アンバインドで元に戻る。

```powershell
usbipd detach --busid 4-2
usbipd unbind --busid 4-2
```

### Step5: WSL側のカメラ設定

WSL側でデバイスを確認する。

```bash
ls /dev/video*

> /dev/video0  /dev/video1
```

バーミッションを設定する

```bash
# videoグループに自分を追加
# Windows側のデタッチ・アンバインド -> バインド・アタッチ によらず1回設定でOK
sudo usermod -aG video $USER

# 権限設定
# Windows側のデタッチ・アンバインド -> バインド・アタッチ で毎回実行
sudo chmod 666 /dev/video0
```

カメラを認識していることを確認する

```bash
sudo apt install v4l-utils
v4l2-ctl --list-devices

> USB Camera:
>         /dev/video0
```

カメラのドライバを確認する

```bash
sudo modprobe uvcvideo
lsmod | grep uvc
```

### Step6: 接続テスト

以下で接続テストする

```bash
python sample0_wsl.py
```

# YOLO とは

**YOLO（You Only Look Once）** は、画像や動画から物体を検出するためのリアルタイム物体検出アルゴリズムです。  
現在は Ultralytics によって開発・公開されている YOLOv8 が広く使われています。

従来の物体検出手法と異なり、画像を一度だけネットワークに通して同時に「位置」と「クラス」を予測するため、非常に高速です。

---

## できること

### 物体検出（Object Detection）

画像内の物体の

- 位置（バウンディングボックス）
- 種類（person, car, dog など）
- 信頼度（confidence）

を同時に推定できます。

例：
- 人物検出
- 車両検出
- 侵入監視
- 人数カウント

---

### リアルタイム処理

GPUを使うことで、Webカメラ映像をリアルタイムで解析できます。

例：
- 監視カメラ
- 転倒検知
- 入退室管理

---

### 画像・動画・カメラ入力対応

```bash
yolo detect predict model=yolov8n.pt source=0      # カメラ
yolo detect predict model=yolov8n.pt source=video.mp4
yolo detect predict model=yolov8n.pt source=image.jpg