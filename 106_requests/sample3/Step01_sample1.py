import os
from CustomRequests import CustomRequests

directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")
os.makedirs(directory,exist_ok=True)

def main():
    url   = "http://127.0.0.1:8080/flaky"
    retry = {"codes": [500, 503], "attempts": 5, "wait": 3}

    try:
        client = CustomRequests()
        response = client.get(url, retry=retry)
        response.raise_for_status()
        print(response.json())
        response.save_local(directory)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
