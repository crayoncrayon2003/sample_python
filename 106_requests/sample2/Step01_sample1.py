import os
import requests
import time

def main():
    url = "http://127.0.0.1:8080/flaky"
    max_retries = 3

    for indx in range(max_retries):
        try:
            print(f"Sending request {indx+1}/{max_retries}")
            response = requests.get(url, timeout=5)

            response.raise_for_status()

            print(response.json())
            break
        except Exception as e:
            time.sleep(1)

if __name__ == "__main__":
    main()
