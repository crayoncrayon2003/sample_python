def deco_tag(tag):
    def _deco_tag(func):
        def wrapper(*args, **kwargs):
            res = '<'+tag+'>'
            res = res + func(*args, **kwargs)
            res = res + '</'+tag+'>'
            return res
        return wrapper
    return _deco_tag

@deco_tag('html')
@deco_tag('body')
def test():
    return 'Hello Decorator!'

def main():
    print(test())

if __name__ == '__main__':
    main()