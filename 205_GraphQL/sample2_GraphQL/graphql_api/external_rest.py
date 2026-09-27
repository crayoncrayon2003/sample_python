import requests

FIXED_API_URL = "http://127.0.0.1:19000"

def set_fixed_data(key: str, value):
    """
    fixed_api にデータをセット
    """
    resp = requests.post(f"{FIXED_API_URL}/set_fixed", json={"key": key, "value": value})
    return resp.json()

def get_fixed_data(key: str):
    """
    fixed_api からデータを取得
    """
    resp = requests.get(f"{FIXED_API_URL}/get_fixed/{key}")
    return resp.json()
