import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result

@retry(
    retry=retry_if_result(lambda r: r is not None and r.status_code in [500, 503]),  # retry condition : status code 500, 503
    stop=stop_after_attempt(3),  # Maximum number of retries
    wait=wait_fixed(3),  # wait for retries
    reraise=True  # Re-throwing Exceptions
)
def send_request(url):
    print("Sending request...")
    response = requests.get(url, timeout=5)

    print(f"Response status code: {response.status_code}")

    return response

def main():
    url = "http://127.0.0.1:8080/flaky"
    try:
        response = send_request(url)
        print(response.json())
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()