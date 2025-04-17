import dataclasses

@dataclasses.dataclass
class COLOR():
    RED  = 'RED'
    BLUE = 'BLUE'
    GREEN= 'GREEN'

@dataclasses.dataclass
class Person:
    name: str
    age : int

def main():
    print('COLOR')
    print(COLOR.RED,COLOR.BLUE,COLOR.GREEN)

    print('Person')
    p = Person('a', 10)
    print(p.name,p.age)

if __name__ == '__main__':
    main()