# cupyとは
NumPyと互換性のあるインターフェースを持ちながら、計算をGPU（NVIDIA製グラフィックボード）に丸投げするためのライブラリです。

# CUDAのバージョン確認
```
$ nvidia-smi
```
または
```
$ nvcc --version
```

# Creating Virtual Environment
```
$ python -m venv env
```

# Activate Virtual Environment
```
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools
(env) $ pip install -r requirements.txt
```

# Deactivate Virtual Environment
```
(env) $ deactivate
```

# Remove Virtual Environment
```
$ rm -rf env
```
