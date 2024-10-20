def main():
    print("------- for / range ------------------ ")
    for idx in range(3, 0, -1):
        print(idx)

    print("-------  for / list ------------------ ")
    items = ["items1", "items2","items3"]
    for item in items:
        print(item)

    print("-------  for / enumerate(list) ------- ")
    items = ["items1", "items2","items3"]
    for idx, item in enumerate(items,1):
        print(idx, item)

    print("-------  while ----------------------- ")
    idx = 0
    while idx < 4 :
        print(idx)
        idx+=1

if __name__ == '__main__':
    main()