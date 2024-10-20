# this is class of Dog
class Dog():
    def sounds(self):
        print('Bowwow')

# this is class of Cat
class Cat():
    def sounds(self):
        print('Meow')

def main():
    # create Dog instance
    dog = Dog()
    # Dog bow-wows.
    dog.sounds()

    # create Cat instance
    cat = Cat()
    # Cat meow.
    cat.sounds()

if __name__ == "__main__":
    main()