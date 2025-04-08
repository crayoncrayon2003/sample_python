from prefect import task, flow

@task
def extract():
    return {"data": [1, 2, 3]}

@task
def transform(raw):
    return [x * 10 for x in raw["data"]]

@task
def load(result):
    print("Loaded:", result)

@flow
def etl_flow():
    raw = extract()
    transformed = transform(raw)
    load(transformed)

if __name__ == "__main__":
    etl_flow()
