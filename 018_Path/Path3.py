import os
import shutil

def main():
    ROOT   = os.path.dirname(os.path.abspath(__file__))
    SUBDIR = os.path.join(ROOT,"SubDir")

    shutil.rmtree(SUBDIR)

if __name__ == "__main__":
    main()