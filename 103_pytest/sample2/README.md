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

# Excec SUT
```
(env) $ cd src/app
(env) $ python3.12 app.py
```

# Excec Test
```
(env) $ cd tests/
(env) $ pytest
```

# Deactivate Virtual Environment
```
(env) $ deactivate
```

# Remove Virtual Environment
```
$ rm -rf env
```