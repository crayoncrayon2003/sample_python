import os
import string
import random
import glob

# create random folder path.
def create_random_path(base_path):
    current_path = base_path
    for _ in range(random.randint(1, 3)):
        folder_name = str(random.randint(1, 3))
        current_path = os.path.join(current_path, folder_name)

    return current_path

# create random file
def create_random_file(path):
    characters = string.ascii_letters + string.digits
    file_name = ''.join(random.choice(characters) for _ in range(5))
    file_ext  = random.choice(['txt', 'csv', 'json'])
    file_path = os.path.join(path, f"{file_name}.{file_ext}")
    with open(file_path, 'w') as file:
        pass
    return file_path

def main():
    # base path
    ROOT   = os.path.dirname(os.path.abspath(__file__))
    SUBDIR = os.path.join(ROOT,"SubDir")

    print("--- Create random path for testing ---")
    paths=[]
    for _ in range(10):
        path = create_random_path(SUBDIR)
        print(path)
        paths.append(path)

    print("--- Create dir/file for testing ---")
    for path in paths:
        # create dirs
        os.makedirs(path, exist_ok=True)
        # create files
        file_path = create_random_file(path)
        print(file_path)

    # get file paths with specified conditions
    files_json = glob.glob(os.path.join(SUBDIR, '**', '*.json'), recursive=True)
    print("--- json ---")
    print(files_json)
    files_txt  = glob.glob(os.path.join(SUBDIR, '**', '*.txt') , recursive=True)
    print("--- text ---")
    print(files_txt)


if __name__ == "__main__":
    main()

