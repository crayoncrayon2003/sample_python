def testDecorator4(func):
    def wrapper(*args, **kwargs):
        res = '<p>'
        res = res + func(args[0], **kwargs)
        res = res + '</p>'
        return res
    return wrapper

@testDecorator4
def test(str):
    return str

def main():
    print(test('Hello Decorator!'))

if __name__ == '__main__':
    main()