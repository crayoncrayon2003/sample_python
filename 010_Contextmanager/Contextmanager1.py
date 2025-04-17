from contextlib import contextmanager

@contextmanager
def log_context(name):
    print(f"Enter: {name}")
    yield
    print(f"Exit: {name}")

def main():
   with log_context("my_func"):
        print("Doing work")

if __name__ == "__main__":
    main()
