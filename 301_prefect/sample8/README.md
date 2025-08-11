# 1. buckend
# 1.1. install
```
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
```

# 1.2. run
```
uvicorn backend.api.main:app --reload
```

check swagger
```
http://127.0.0.1:8000/docs
```

check rest api
```
curl -X GET "http://127.0.0.1:8000/api/v1/plugins/"
```

# 1.3. uninstall
```
deactivate
rm -rf env
```


# 2. Front
```
node -v
> v20.x.x
```

## 2.1 install
```
npm install
npm install @rjsf/core @rjsf/validator-ajv8 @rjsf/mui @mui/material@^6.5.0 @mui/icons-material@^6.5.0 @emotion/react @emotion/styled
```

## 2.2 run
```
npm run dev
```

# 2.3. uninstall
```
rm -rf node_modules
rm package-lock.json
```