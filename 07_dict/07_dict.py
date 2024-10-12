from collections import defaultdict

def main():
    items1 = {"items0":0, "items2":2, "items3":3}
    items2 = {"items4":4, "items5":2, "items0":0}

    print("------- print dict -------")
    print(items1)

    print("------- print count ------")
    print(len(items1))

    print("------- change value -----")
    items1["items0"] = -1
    print(items1)

    print("------- get value -----")
    print(items1.get('items-1'))

    print("------- append value -----")
    items1["items1"] = 1
    print(items1)

    print("------- pop value --------")
    v = items1.pop("items0")
    print(v)
    print(items1)

    print("------- pop popitem ------")
    k, v = items1.popitem()
    print(k, v)
    print(items1)

    print("------- update dict ------")
    items1.update(items2)
    print(items1)

    print("------- del items0 -------")
    del items1['items0']
    print(items1)

    print("------- setdefault case1 : Set the value if the key does not exist.")
    items1.setdefault('items0', 0)
    print(items1)
    print("------- setdefault case2 : Not set the value if the key does exist.")
    items1.setdefault('items0', 1)
    print(items1)

    print("------- print keys ------")
    for key in items1.keys():
        print(key)

    print("------- print values ----")
    for value in items1.values():
        print(value)

    print("------- print items ------")
    for key, value in items1.items():
        print(key, value)


    print("------- clear ------------")
    items1.clear()
    print(items1)



    print("------- fromkeys case1 ----")
    x = ('key1', 'key2', 'key3')
    y = 0
    items3 = dict.fromkeys(x, y)
    print(items3)

    print("------- fromkeys case2----")
    x = ('key1', 'key2', 'key3')
    items4 = dict.fromkeys(x)
    print(items4)

    print("------- defaultdict ------")
    items5 = defaultdict(list)
    for key in ["key1" ,"key2"]:
        for value in range(1,5,1):
            # List Methods are available.
            items5[key].append(value)
    print(items5)

if __name__ == '__main__':
    main()