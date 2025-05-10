import os
import json
from CustomRequests import CustomRequests

directory_root = os.path.dirname(os.path.abspath(__file__))
directory_save = os.path.join(directory_root, "temp")
os.makedirs(directory_save,exist_ok=True)

def main():
    url   = "http://127.0.0.1:8080/csv"
    retry = {"codes": [500, 503], "attempts": 5, "wait": 3}

    try:
        client = CustomRequests()
        response = client.get(url, retry=retry)
        response.raise_for_status()
        response.save_local(directory_save)

        dump = response.dump_template(directory_root, "template.j2")
        print(json.dumps(json.loads(dump), ensure_ascii=False, indent=4))

        dump = response.dump_sql(directory_root, "query.sql")
        print(json.dumps(json.loads(dump), ensure_ascii=False, indent=4))
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
