from taskflow import engines, task
from taskflow.patterns import unordered_flow

class Task1(task.Task):
    def execute(self):
        print("My name is Task1")
        return "T1"

class Task2(task.Task):
    def execute(self):
        print("My name is Task2")
        return "T2"

class Task3(task.Task):
    def execute(self, data1, data2):
        print("My name is Task3")
        print(f"input param: {data1}")
        print(f"input param: {data2}")
        return "T3"

def main():
    # create task
    task1 = Task1(provides="data1")
    task2 = Task2(provides="data2")
    task3 = Task3(requires=["data1", "data2"])

    # create workflow
    workflow = unordered_flow.Flow("ParallelFlow") \
        .add(task1) \
        .add(task2) \
        .add(task3)

    # mapping of data
    engine = engines.load(workflow, store={"data1": task1.execute(), "data2": task2.execute()})

    # run
    engine.run()

if __name__ == "__main__":
    main()
