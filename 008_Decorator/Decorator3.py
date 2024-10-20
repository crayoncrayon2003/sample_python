def deco_html(func):
    def wrapper(*args, **kwargs):
        res = '<html>'
        res = res + func(*args, **kwargs)
        res = res + '</html>'
        return res
    return wrapper

def deco_body(func):
    def wrapper(*args, **kwargs):
        res = '<body>'
        res = res + func(*args, **kwargs)
        res = res + '</body>'
        return res
    return wrapper

@deco_html
@deco_body
def test():
    return 'Hello Decorator'

def main():
    print(test())

if __name__ == '__main__':
    main()