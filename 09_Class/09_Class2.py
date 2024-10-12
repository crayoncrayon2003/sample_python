class myClass():
    def __init__(self):
        print("this is Constructor")
        self.public_var     = 1 # Public variable.
        self._private_var   = 2 # Private variable.
        self.__internal_var = 3 # internal variable.

        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        print("internal var :",self.__internal_var)

    def publicFunc(self):
        print("this is public Func")
        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        # print("internal var :",self.__internal_var) # this is error

    def _privateFunc(self):
        print("this is private Func")
        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        # print("internal var :",self.__internal_var) # this is error

    def __internalFunc(self):
        print("this is internal Func")
        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        # print("internal var :",self.__internal_var) # this is error

    def __del__(self):
        print("this is Destructor")

class mySubClass(myClass):
    def __init__(self):
        self.public_var     = 10 # Public variable.
        self._private_var   = 20 # Private variable.
        self.__internal_var = 30 # Private variable.

        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        print("internal var :",self.__internal_var)

    def publicFunc(self):
        print("this is mySubClass public Func")
        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        # print("internal var :",self.__internal_var) # this is error

    def _privateFunc(self):
        print("this is mySubClass private Func")
        print("public  var  :",self.public_var)
        print("private var  :",self._private_var)
        # print("internal var :",self.__internal_var) # this is error


    def __del__(self):
        print("this is Destructor")


def main():
    c1 = myClass()
    c1.publicFunc()
    c1._privateFunc()
    # c1.__internalFunc() # this is error

    c2 = mySubClass()
    c2.publicFunc()
    c2._privateFunc()
    # c2.__internalFunc() # this is error


if __name__ == '__main__':
    main()