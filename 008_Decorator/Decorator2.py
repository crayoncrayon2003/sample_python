def testDecorator1(func):
    def wrapper(*args, **kwargs):
        print('--start--')
        func(*args, **kwargs)
        print('--end--')
    return wrapper

def testDecorator2(func):
    import os
    def wrapper(*args,**kwargs):
        res = '--start--' + os.linesep
        res += func(*args,**kwargs) + '!' + os.linesep
        res += '--end--'
        return res
    return wrapper

@testDecorator1
def test1():
    print('Hello Decorator')

@testDecorator2
def test2():
    return('Hello Decorator')

def main():
    print("---test1---")
    test1()

    print("---test2---")
    print(test2())

if __name__ == '__main__':
    main()