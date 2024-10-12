import yaml

variables = {}

# 変数セット命令
def _set(variable_name, value):
    variables[variable_name] = value;

# 引数を評価する関数
def evaluate_args(args):
    result = []
    for arg in args:
        if arg in variables:
            result.append(variables[arg])
        else:
            result.append(arg)
    return result

# DSLから呼び出し可能な関数を列挙
func_dict = {'print': print, 'set': _set}

with open('hello2.yml') as yaml_file:
    # YAMLのロード
    dsl = yaml.load(yaml_file)
    # ロードされたDSLを標準出力
    print(dsl)
    # 実際の解釈処理
    # トップレベルをリストに変更。instはinstructionの意味
    for inst in dsl:
        if 'func' in inst and 'args' in inst:
            func_name = inst['func']
            if func_name in func_dict:
                func = func_dict[func_name]
                # 引数を評価
                args = evaluate_args(inst['args'])
                func(*args)