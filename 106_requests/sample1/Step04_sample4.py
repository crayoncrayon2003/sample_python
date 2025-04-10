import os
import requests

def main():
    url     = "http://127.0.0.1:8080/sleep"
    method  = "GET"
    headers = {"Accept" : "application/json"}
    query   = {"test"   : "test" }
    timeout = 10

    try:
        response = requests.request(method, url=url, headers=headers, params=query, timeout=timeout)

        print("Status Code:", response.status_code)
        print("Response JSON:")
        print(response.json())

    except requests.exceptions.Timeout:
        print("Timeout")

    except requests.exceptions.ConnectionError:
        print("ConnectionError")
    except Exception as e:
        print("others")
        print("e")

if __name__ == "__main__":
    main()
