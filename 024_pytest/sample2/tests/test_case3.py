import json
import requests

def test_case3_1():
    api_calls = [
        ( ("GET",  "http://localhost:8080/data-api", {'Accept'      : 'application/json'},  {"id": 1}, None                  ), (404, '{\n  "id": 1,\n  "message": "id not found"\n}\n') ),
        ( ("POST", "http://localhost:8080/data-api", {'Content-Type': 'application/json'},  None,      {"id": 1, "value": 10}), (200, "") ),
        ( ("GET",  "http://localhost:8080/data-api", {'Accept'      : 'application/json'},  {"id": 1}, None                  ), (200, "") )
    ]

    for index, (req, exp) in enumerate(api_calls):
        try:
            if req[4] is None:
                response = requests.request(req[0], url=req[1], headers=req[2], params=req[3], timeout=5)
            else:
                response = requests.request(req[0], url=req[1], headers=req[2], params=req[3], data=json.dumps(req[4]), timeout=5)

            assert response.status_code == exp[0]
            assert response.text == exp[1]
        except AssertionError as e:
            print(f"Assertion failed at table index {index}: {e}")
            break


if __name__ == "__main__":
    test_case3_1()

