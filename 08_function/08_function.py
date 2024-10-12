def toSeconds(hour, min, sec):
    answer = hour * 3600 + min * 60 + sec;
    return(answer);

def main():
    sec1 = toSeconds(12, 34, 56)
    print(sec1)


if __name__ == '__main__':
    main()