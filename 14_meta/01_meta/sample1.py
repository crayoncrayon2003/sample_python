# クラス「Dog」
class Dog():
    def sounds(self):
        print('Bowwow')

# クラス「Cat」
class Cat():
    def sounds(self):
        print('Meow')

def main():
    # クラス「Dog」のインスタンスを作る
    dog = Dog()
    # クラス「Dog」のsoundsメソッド実行
    dog.sounds()

    # クラス「Cat」のインスタンスを作る
    cat = Cat()
    # クラス「Cat」のsoundsメソッド実行
    cat.sounds()

if __name__ == "__main__":
    main()