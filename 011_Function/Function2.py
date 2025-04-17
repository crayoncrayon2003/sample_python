import inspect

def myfunc1():
    # arg is void
    # retun is void
    print("I'm   ", inspect.currentframe().f_code.co_name)
    info = inspect.getargvalues(inspect.currentframe())
    for key in info.args:
        print("{0} : value = {1}, type = {2}".format(key,info.locals[key], type(info.locals[key])))

def myfunc2(param1):
    # arg is param1
    # retun is void
    print("I'm   ", inspect.currentframe().f_code.co_name)
    info = inspect.getargvalues(inspect.currentframe())
    for key in info.args:
        print("{0} : value = {1}, type = {2}".format(key,info.locals[key], type(info.locals[key])))

def myfunc3(param1,param2):
    # arg is param1, param2
    # retun is void
    print("I'm   ", inspect.currentframe().f_code.co_name)
    info = inspect.getargvalues(inspect.currentframe())
    for key in info.args:
        print("{0} : value = {1}, type = {2}".format(key,info.locals[key], type(info.locals[key])))

def myfunc4():
    # arg is void
    # retun is value
    print("I'm   ", inspect.currentframe().f_code.co_name)
    info = inspect.getargvalues(inspect.currentframe())
    for key in info.args:
        print("{0} : value = {1}, type = {2}".format(key,info.locals[key], type(info.locals[key])))

    ret1 = 100
    return ret1

def myfunc4():
    # arg is void
    # retun is list
    print("I'm   ", inspect.currentframe().f_code.co_name)
    info = inspect.getargvalues(inspect.currentframe())
    for key in info.args:
        print("{0} : value = {1}, type = {2}".format(key,info.locals[key], type(info.locals[key])))

    ret1 = 100
    ret2 = 100
    return [ret1, ret2]

def main():
    ret = myfunc1()
    print("ret = ", ret, "type = ", type(ret), "\n")

    ret = myfunc2(1)
    print("ret = ", ret, "type = ", type(ret), "\n")

    # arg1 is value, arg2 is value
    ret = myfunc3(1,2)
    print("ret = ", ret, "type = ", type(ret), "\n")

    # arg1 is value, arg2 is None
    ret = myfunc3(1,None)
    print("ret = ", ret, "type = ", type(ret), "\n")

    # arg1 is value, arg2 is list
    ret = myfunc3(1,[1,2,3])
    print("ret = ", ret, "type = ", type(ret), "\n")

    ret = myfunc4()
    print("ret = ", ret, "type = ", type(ret), "\n")


if __name__ == '__main__':
    main()