# 1. Install

## 1.1. Creating Virtual Environment

```bash
python3 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) pip install -r requirements.txt
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
