def func(self, *args):
    print(self.kind)

def main():
    for kind in ['Bowwow', 'Meow']:
        # dynamically create Animal class at runtime.
        Animal = type(
            'animal',
            (),
            {
                'kind': kind,
                'sounds': func
            }
        )

        # create AnimalC instance
        temp = Animal()
        # 1st Bowwow, 2nd Meow
        temp.sounds()

if __name__ == "__main__":
    main()