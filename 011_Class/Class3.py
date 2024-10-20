class myClass():
    def __init__(self):
        print("this is Constructor")
        self._myVar = 1

    @property
    def myVar(self):
        # this is getter
        return self._myVar

    @myVar.setter
    def myVar(self, value):
        # this is setter
        self._myVar = value

    @myVar.deleter
    def myVar(self):
        # this is deleter
        del self._myVar

def main():
    c1 = myClass()
    print(c1.myVar)

    c1.myVar = 100
    print(c1.myVar)

    del c1.myVar
    # print(c1.myVar)

if __name__ == '__main__':
    main()