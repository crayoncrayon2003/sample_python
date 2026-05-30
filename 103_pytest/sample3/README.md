# Creating Virtual Environment
```
$ python -m venv env
```

# Activate Virtual Environment
```
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools wheel
(env) $ pip install -r requirements.txt
```

# Run tests on all files.
```
(env) $ pytest
```

# Run tests on all files. Output detailed information
```
(env) $ pytest -vv -s
```

# Run tests on one file
```
(env) $ pytest -vv -s tests/test_sample_client.py
```

# Run tests only a specific test function
```
(env) $ pytest -vv -s tests/test_sample_client.py::test_fetch_and_store
```

# Remove Virtual Environment
```
(env) $ deactivate
$ rm -rf env
```
