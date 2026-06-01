# 1. Virtual Environment
## 1.1. Creating Virtual Environment
```bash
$ python3.12 -m venv env
```

## 1.2. Activate Virtual Environment
```bash
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools
(env) $ pip install -r requirements.txt
(env) $ ipython kernel install --user --name=env
(env) $   >  Installed kernelspec env in /your/dir/path/env
```

## 1.3. show jupyter kerne list
```bash
(env) $ jupyter kernelspec list
```

## 1.4. remove jupyter kerne list
```bash
(env) $ jupyter kernelspec remove env
```

## 1.5. Deactivate Virtual Environment
```bash
(env) $ deactivate
```

## 1.6. Remove Virtual Environment
```bash
$ rm -rf env
```


# 2. System-wide Jupyter
## 2.1. Install Dependencies
```bash
sudo apt update
sudo apt upgrade
sudo apt install jupyter-notebook jupyter-client jupyter-core jupyter-nbconvert
```

## 2.3. Confirm
```bash
jupyter notebook --version
jupyter kernelspec list
```

# 3. Font
## 3.1. IPA fonts (Gothic and Mincho)
```bash
sudo apt install fonts-ipafont
```

## 3.2. IPAex fonts (IPA Gothic / IPA P Gothic - legacy IPA fonts)
```bash
sudo apt install fonts-ipaexfont
```

## 3.3. IPAex fonts (IPA Gothic / IPA P Gothic - improved version of IPA fonts)
```bash
sudo apt install fonts-ipaexfont-gothic
```

## 3.4. Noto CJK (Chinese, Japanese, Korean support)
```bash
sudo apt install fonts-noto-cjk
```

# 4. VS Code Stting
## 4.1. Install Extensions
Side Menu -> Extensions -> Search and Installation follow
* Python
* Jupyter

## 4.2. Activate Virtual Environment
```
push   : Ctrl + Shift + p
input  : Python: Select Interpreter
select : env (Virtual Environment)
```

## 4.3. Create Notebook
### 4.3.1. case1
```
push   : Ctrl + Shift + p
input  : jupyter: Create Interactive Window
```

### 4.3.2. case2
```
Create New File *.ipynb
```
