import yaml
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(ROOT,"hello1.yml")

def main():
    # Available function of this program
    func_dict = {'print': print}

    with open(FILE) as yaml_file:
        # load 'hello1.yml'
        dsl = yaml.load(yaml_file)
        # print 'hello1.yml'
        print(dsl)

        # 実際の解釈処理
        if 'func' in dsl and 'args' in dsl: # if include 'func' and 'args' in 'hello1.yml'
            func_name = dsl['func']         # get function for keyword 'func' in 'hello1.yml'
            if func_name in func_dict:      # Available function of this program
                func = func_dict[func_name]
                func(*dsl['args'])          # exec function for keyword 'args' in 'hello1.yml'

if __name__ == "__main__":
    main()