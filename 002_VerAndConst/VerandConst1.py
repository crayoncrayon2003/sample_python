def main():
    var_A   = "1"
    var_B   = 2
    CONST_C = "3"
    CONST_D = 4

    print("this is var value" + var_A)
    print("this is var type"  , type(var_A))
    print("this is var id"    , id(var_A))

    print("this is var value" , var_B)
    print("this is var type"  , type(var_B))
    print("this is var id"    , id(var_B))

    print("this is const {} , {}".format(CONST_C, CONST_D))



if __name__ == '__main__':
    main()