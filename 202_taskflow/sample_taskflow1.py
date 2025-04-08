from taskflow import engines, task
from taskflow.patterns import linear_flow

class Task1(task.Task):
    def execute(self):
        print("My name is Task1")
        return "T1"

class Task2(task.Task):
    def execute(self, data):
        print("My name is Task2")
        print(f"input param: {data}")
        return "T2"

def main():
    # create workflow
    workflow = linear_flow.Flow("SampleFlow") \
        .add(Task1(provides="data")) \
        .add(Task2(requires="data"))

    # load workflow
    engine = engines.load(workflow)

    # run
    engine.run()

if __name__ == "__main__":
    main()
