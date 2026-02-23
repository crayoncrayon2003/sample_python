# 1. Install

## 1.1. Creating Virtual Environment
```bash
python3 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) pip install scikit-learn numpy pandas matplotlib
(env) pip install jupyter ipykernel
(env) python3 -m ipykernel install --user --name=env --display-name "Python (env)"
```

## 1.2. Confirm
```bash
(env) python3 -c "import sklearn; print(sklearn.__version__)"
(env) jupyter kernelspec list
```

## 1.3. VS Code Extensions
* Python
* Jupyter

## 1.4. Deactivate
```bash
(env) deactivate
```

## 1.5. Remove
```bash
rm -rf env
```

#  scikit-learn とは
Python の機械学習ライブラリの定番です。

ディープラーニングは対象外ですが、それ以外の古典的な機械学習アルゴリズムがほぼ網羅されています。

## できること
### 分類（Classification）
データがどのクラスに属するかを予測します。

### 回帰（Regression）
数値を予測します。住宅価格の予測、売上予測、気温予測などが代表例です。

### クラスタリング（Clustering）
ラベルなしのデータを自動でグループ分けします。顧客のセグメント分け、異常検知などに使われます。

### 次元削減（Dimensionality Reduction）
高次元のデータを低次元に圧縮して可視化や前処理に使います。PCA（主成分分析）が代表的です。

### 特徴量エンジニアリング・前処理
標準化・正規化・欠損値補完・カテゴリ変数のエンコードなど、モデルに渡す前のデータ加工が充実しています。

### モデル評価・チューニング
交差検証、グリッドサーチによるハイパーパラメータ探索、精度・F1スコアなどの評価指標が揃っています。

## 苦手なこと
* 画像・音声・テキストなどのディープラーニングは対象外（→ PyTorch / TensorFlow の領域）
* 大規模データの処理は得意ではない（→ Spark ML などが適切）
* GPU を使った高速化には対応していない