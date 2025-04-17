def wrapper(func):
    def inner(*args, **kwargs):
        print("Before call")
        result = func(*args, **kwargs)
        print("After call")
        return result
    return inner

def greet1():
    print("Hello")

@wrapper
def greet2():
    print("Hello")

def main():
    # wrapper = Wrapping a function
    wrapped = wrapper(greet1)
    wrapped()

    # decorator = decorating a function
    greet2()


if __name__ == "__main__":
    main()
