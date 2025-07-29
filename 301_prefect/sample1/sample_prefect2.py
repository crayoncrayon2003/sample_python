from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

# name=None                       # TaskName
# retries=0                       # Number of retries
# retry_delay_seconds=0           # Retry Interval (sec)
# persist_result=False            # Persistence of results
# cache_key_fn=None               # cache key (ex. task name
# cache_expiration=None           # cache exp
# log_prints=False                #
# description=None                #
# tags=None                       #

@task(retries=3, retry_delay_seconds=5, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fragile_task(x):
    if x % 2 == 0:
        raise ValueError("Error")
    return x * 10

@flow
def robust_flow():
    for i in range(0,5,1):
        print(f"----------- loop : {i} ----------- ")
        try:
            result = fragile_task(i)
            print(f"success: {result}")
        except Exception as e:
            print(f"failure: {e}")

if __name__ == "__main__":
    robust_flow()

