def func(self, *args):
    print(self.kind)

def main():
    for kind in ['Bowwow', 'Meow']:
        # クラス「Animal」　クラス自体を動的に作る
        Animal = type(
            'animal',
            (),
            {
                'kind': kind,
                'sounds': func
            }
        )

        # クラス「AnimalClass」のインスタンスを作る
        temp = Animal()
        # クラス「AnimalClass」のsoundsメソッド実行
        temp.sounds()

if __name__ == "__main__":
    main()