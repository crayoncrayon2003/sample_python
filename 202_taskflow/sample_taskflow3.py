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
    task1 = Task1()
    task2 = Task2()
    task3 = Task3()

    # create workflow
    workflow = unordered_flow.Flow("ParallelFlow")
    workflow.add(task1, task2, task3)
    task3.requires(task1, task2)

    # load workflow
    engine = engines.load(workflow)

    # run
    engine.run()

if __name__ == "__main__":
    main()
