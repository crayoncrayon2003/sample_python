from prefect import flow, task

@task
def process_item(x):
    return x * 2

@flow
def map_flow():
    data = [1, 2, 3, 4]
    results = process_item.map(data)
    print("結果:", results)

map_flow()
