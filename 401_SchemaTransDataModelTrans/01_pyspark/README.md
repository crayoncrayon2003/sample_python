# create env
```
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

# Sample1
## Run
```
python3.12 sample1.py
```

# Sample2
## Run
```
python3.12 sample2_1.py
python3.12 sample2_2.py
python3.12 sample2_3.py
```

# Sample3
## build
```
docker compose up -d
```

wait for 5 minutes.
Access the following URL using the Web browser.
```
http://localhost:21000/login.jsp
```
* user: admin
* padd: admin

## Run
```
python3.12 sample3_1.py
python3.12 sample3_2.py
python3.12 sample3_3.py
```

# Sample4
## Run
```
python3.12 sample4_1.py
python3.12 sample4_2.py
python3.12 sample4_3.py
```

# delete env
```
(env) deactivate
rm -R env
```