import os

def main():
    ROOT   = os.path.dirname(os.path.abspath(__file__))
    SUBDIR = os.path.join(ROOT,"SubDir")

    print("this ptyhon file path  ",ROOT)
    print("this ptyhon file path  ",SUBDIR)

if __name__ == "__main__":
    main()