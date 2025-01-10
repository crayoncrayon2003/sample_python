import requests
import json

def main():
    # create request data
    url = "http://localhost:5000/transform"
    headers = {"Content-Type": "application/json"}
    data = {
        "table_source": "default.source_schema@hive",
        "schema_source": "source_schema",
        "table_target": "default.target_schema@hive",
        "schema_target": "target_schema",
        "data": [
            {"Name": "Alice", "Age": 25},
            {"Name": "Bob", "Age": 30},
            {"Name": "Cathy", "Age": 28}
        ]
    }
    print("Request:")
    print(json.dumps(data, indent=4))

    response = requests.post(url, headers=headers, data=json.dumps(data))

    # show response
    if response.status_code == 200:
        print("Response:")
        print(json.dumps(response.json(), indent=4))
    else:
        print(f"Error {response.status_code}: {response.text}")


if __name__ == "__main__":
    main()