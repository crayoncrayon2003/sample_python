# Creating Virtual Environment
```
$ python3.12 -m venv env
```

# Activate Virtual Environment
```
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools
(env) $ pip install -r requirements.txt
(env) $ ipython kernel install --user --name=env
(env) $   >  Installed kernelspec env in /your/dir/path/env
```

## show jupyter kerne list
```
(env) $ jupyter kernelspec list
```

## remove jupyter kerne list
```
(env) $ jupyter kernelspec remove env
```

# VS Code Stting
## Install Extensions
Side Menu -> Extensions -> Search and Installation follow
* Python
* Jupyter

## Activate Virtual Environment
```
push   : Ctrl + Shift + p
input  : Python: Select Interpreter
select : env (Virtual Environment)
```

## Create Notebook
### case1
```
push   : Ctrl + Shift + p
input  : jupyter: Create Interactive Window
```

### case2
```
Create New File *.ipynb
```

## Exec
...


# Deactivate Virtual Environment
```
(env) $ deactivate
```

# Remove Virtual Environment
```
$ rm -rf env
```

