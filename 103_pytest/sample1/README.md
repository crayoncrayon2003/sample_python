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

# Excec Test
## run stub server
```
(env) $ cd tests/
(env) $ python stub_server.py
```

## run test
### Case1 :
```
(env) $ cd tests/
(env) $ pytest
```

### Case2 : Supports pytest-xdist
```
(env) $ cd tests/
(env) $ pytest -n 2
```

### Case3 : Check for compliance with the Python Code Style Guide (PEP 8)
```
(env) $ cd tests/
(env) $ pytest --flake8
```

### Case4 : Outputs test results as HTML reports
```
(env) $ cd tests/
(env) $ pytest --html=report.html
```

### Case5 : Measuring test coverage
```
(env) $ cd tests/
(env) $ pytest --cov=main
```

### Case6 : Code complexity analysis tool
```
(env) $ cd src/
(env) $ lizard main.py
```


# Deactivate Virtual Environment
```
(env) $ deactivate
```

# Remove Virtual Environment
```
$ rm -rf env
```