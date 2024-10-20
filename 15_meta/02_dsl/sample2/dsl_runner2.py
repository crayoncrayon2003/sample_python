import yaml
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(ROOT,"hello2.yml")

variables = {}

# store args
def _set(variable_name, value):
    variables[variable_name] = value;

# evaluate args
def evaluate_args(args):
    result = []
    for arg in args:
        if arg in variables:
            result.append(variables[arg])
        else:
            result.append(arg)
    return result

def main():
    # Available function of this program
    func_dict = {'print': print, 'set': _set}

    with open(FILE) as yaml_file:
        # load 'hello2.yml'
        dsl = yaml.load(yaml_file)
        # print 'hello1.yml'
        print(dsl)

        # Support for multiple functions
        for inst in dsl:
            if 'func' in inst and 'args' in inst:       # if include 'func' and 'args' in 'hello1.yml'
                func_name = inst['func']                # get function for keyword 'func' in 'hello1.yml'
                if func_name in func_dict:              # Available function of this program
                    func = func_dict[func_name]
                    args = evaluate_args(inst['args'])  # evaluate args
                    func(*args)                         # exec function for keyword 'args' in 'hello1.yml'

if __name__ == "__main__":
    main()