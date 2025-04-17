from abc import ABC, abstractmethod

# Abstract Class
class Animal(ABC):
    @abstractmethod
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):   # speakを強制
        print("Woof!")

# Meta Class
class MyMeta(type):
    def __new__(cls, name, bases, dct):
        dct['hello'] = lambda self: print("Hello from metaclass!")
        return super().__new__(cls, name, bases, dct)

class MyClass(metaclass=MyMeta):
    pass


def main():
    dog = Dog()
    dog.speak()

    obj = MyClass()
    obj.hello()


if __name__ == "__main__":
    main()
