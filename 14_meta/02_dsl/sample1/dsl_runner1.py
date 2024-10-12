import yaml

# DSLから呼び出し可能な関数を列挙
func_dict = {'print': print}

with open('hello1.yml') as yaml_file:
    # YAMLのロード
    dsl = yaml.load(yaml_file)
    # ロードされたDSLを、確認のために標準出力
    print(dsl)
    # 実際の解釈処理
    if 'func' in dsl and 'args' in dsl:
        func_name = dsl['func']
        if func_name in func_dict:
            func = func_dict[func_name]
            func(*dsl['args'])