import os
import duckdb

directory = os.path.dirname(os.path.abspath(__file__))
database_path = os.path.join(directory, "project", "test_project", "dev.duckdb")

con = duckdb.connect(database_path)
result = con.execute("SELECT * FROM main.csv_to_ngsi_model").fetchall()
print(result)