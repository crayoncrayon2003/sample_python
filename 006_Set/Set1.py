def main():
    items1 = {'items0', 'items1', 'items2', 'items3', 'items3'}
    items2 = {'items0', 'items5'}

    print("------- print set -------")
    print(items1)

    print("------- print set ------")
    print(len(items1))

    print("------- pop value --------")
    # remove any value
    v = items1.pop()
    print(items1)

    print("------- add value -----")
    items1.add(v)
    print(items1)

    print("------- discard items0 ----")
    items1.discard('items0')
    items1.discard('items0')
    print(items1)

    print("------- remove items1 ----")
    items1.remove('items1')
    print(items1)

    print("------- items1 | items2 --")
    print(items1.union(items2))

    print("------- update items1 --")
    items1.update(items2)
    print(items1)

    print("------- items1 & items2 --")
    print(items1.intersection(items2))

    print("------- update items1 --")
    items1.intersection_update(items2)
    print(items1)

    print("------- items1 - items2 --")
    print(items1.difference(items2))

    print("------- update items1 --")
    items1.difference_update(items2)
    print(items1)



    print("------- isdisjoint --")
    print({0,1}.isdisjoint({2,3}))
    print({0,1}.isdisjoint({1,2}))

    print("------- issubset --")
    print({0,1,2,3}.issubset({0,1}))
    print({0,1}.issubset({0,1,2,3}))

    print("------- issuperset --")
    print({0,1,2,3}.issuperset({0,1}))
    print({0,1,2,3}.issuperset({4,5}))

    print("------- symmetric_difference --")
    print({0,1,2,3}.symmetric_difference({0,1}))
    print({0,1,2,3}.symmetric_difference({4,5}))


    print("------- loop -------------")
    for idx, item in enumerate(items1,1):
        print(idx, item)

    print("------- clear ------------")
    items1.clear()
    print(items1)



if __name__ == '__main__':
    main()