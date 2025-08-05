# scripts/tasks/validation/duckdb_validator.py
from pathlib import Path
from prefect import task
import duckdb
import yaml
from jinja2 import Environment, FileSystemLoader

@task
def duckdb_validator(config_path: str):
    """
    設定ファイルと検証用クエリテンプレートに基づき、DuckDBでデータ検証を実行する。
    違反した行が1行でもあればエラーを送出する。
    """
    print(f"Validation: Running checks defined in '{config_path}'...")
    
    config_path_obj = Path(config_path)
    with open(config_path_obj, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    query_template_path = Path(config["paths"]["validation_query"])
    
    # テンプレートをレンダリング
    env = Environment(loader=FileSystemLoader(str(query_template_path.parent)), autoescape=True)
    template = env.get_template(query_template_path.name)
    query = template.render(config=config)
    
    # DuckDBで検証クエリを実行
    con = duckdb.connect(database=":memory:")
    try:
        result = con.execute(query).fetchall()
    finally:
        con.close()

    if result:
        raise ValueError(f"{len(result)} rows failed validation. First error row: {result[0]}")
    
    print("Validation successful: All data conforms to the rules.")