import os
import requests

def main():
    url= "http://127.0.0.1:8080/hello"
    response = requests.get(url)

    print("Status code:")
    print(response.status_code)

    print("Headers:")
    for key, value in response.headers.items():
        print(f"{key}: {value}")

    print("Response Text:")
    print(response.text)

    print("Response JSON:")
    print(response.json())

if __name__ == "__main__":
    main()
