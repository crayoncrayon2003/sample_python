import os
import json
import duckdb
import pandas as pd

# load query
def load_sql_query(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

# save schema/data
def save_duckdb_table(df, database_path, table_name):
    conn = duckdb.connect(database=database_path)
    conn.register('temp_table', df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_table")
    conn.close()

# load schema/data
def load_duckdb_table(database_path, table_name):
    conn = duckdb.connect(database=database_path)
    query = f"SELECT * FROM {table_name}"
    df = conn.execute(query).df()
    conn.close()
    return df

# get fields/records for data.json
def extract_fields_and_records_from_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        fields = data["data"]["fields"]
        records = data["data"]["records"]
        return [fields, records]

# create schema from fields
def create_schema(fields):
    columns = [field["id"] for field in fields]
    return columns

# create pandas DataFrame
def create_dataframe(records, columns):
    df = pd.DataFrame(records, columns=columns)
    return df

# convert data
def convert(df, file_path_sql):
    sql_query = load_sql_query(file_path_sql)
    conn = duckdb.connect()
    conn.register("df", df)
    result_df = conn.execute(sql_query).df()
    conn.close()
    return result_df

# Parse JSON strings into proper JSON objects
def parse_json_columns(df, json_columns):
    for col in json_columns:
        df[col] = df[col].apply(json.loads)
    return df

if __name__ == "__main__":
    # ###########################
    # base dir
    directory = os.path.dirname(os.path.abspath(__file__))
    database_path = os.path.join(directory, "database.duckdb")

    # ###########################
    # convert csv
    file_path = os.path.join(directory, "data.csv")
    df = pd.read_csv(file_path)

    # create DuckDB
    save_duckdb_table(df, database_path, "csv_table")
    df = load_duckdb_table(database_path, "csv_table")

    # convert
    file_path_sql = os.path.join(directory, "query_duck.sql")
    cnv_df = convert(df, file_path_sql)

    # parse json
    json_columns = ["data1", "data2", "data3"]
    cnv_df = parse_json_columns(cnv_df, json_columns)

    # save json
    output_path = os.path.join(directory, "output_duckdb_csv.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cnv_df.to_dict(orient="records"), f, ensure_ascii=False, indent=4)

    # ###########################
    # convert json
    file_path = os.path.join(directory, "data.json")
    [fields, records] = extract_fields_and_records_from_json(file_path)

    columns = create_schema(fields)
    df = create_dataframe(records, columns)

    # create DuckDB
    save_duckdb_table(df, database_path, "json_table")
    df = load_duckdb_table(database_path, "json_table")

    # convert
    cnv_df = convert(df, file_path_sql)

    # parse json
    cnv_df = parse_json_columns(cnv_df, json_columns)

    # save json
    output_path = os.path.join(directory, "output_duckdb_json.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cnv_df.to_dict(orient="records"), f, ensure_ascii=False, indent=4)
