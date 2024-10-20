import collections

def main():
    items1 = ['items0', 'items2', 'items3']
    items2 = ['items4', 'items5', 'items0']

    print("------- print list -------")
    print(items1)

    print("------- print count ------")
    print(len(items1))

    print("------- print index ------")
    print(items1.index('items2'))

    print("------- change value -----")
    items1[0] = "items1"
    print(items1)

    print("------- append value -----")
    items1.append("items4")
    print(items1)

    print("------- pop value --------")
    items1.pop()
    print(items1)

    print("------- extend list ------")
    items1.extend(items2)
    print(items1)

    print("------- insert value -----")
    items1.insert(0,"items0")
    print(items1)

    print("------- count items0 -----")
    print(items1.count("items0"))

    print("------- remove items0 ----")
    items1.remove('items0')
    print(items1)

    print("------- sort -------------")
    items1.sort(reverse=True)
    print(items1)

    print("------- reverse ----------")
    items1.reverse()
    print(items1)

    print("------- loop -------------")
    for idx, item in enumerate(items1,1):
        print(idx, item)

    print("------- clear ------------")
    items1.clear()
    print(items1)
    
    collections.defaultdict(int)

if __name__ == '__main__':
    main()