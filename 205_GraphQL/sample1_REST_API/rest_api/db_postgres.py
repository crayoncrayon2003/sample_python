import psycopg2
import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "0.0.0.0")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "testdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

def _get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def init_postgres():
    """orders と users テーブルが存在しない場合に作成"""
    conn = _get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT PRIMARY KEY,
            item TEXT NOT NULL,
            quantity INT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

def insert_sample_user():
    conn = _get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING")
    conn.commit()
    cur.close()
    conn.close()

def get_postgres_data():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()
    # ordersテーブルの全件を取得
    cur.execute("SELECT id, item, quantity FROM orders ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # 辞書リストで返す
    return [{"id": r[0], "item": r[1], "quantity": r[2]} for r in rows]

def set_postgres_data(data_list):
    """orders テーブルにデータを挿入"""
    conn = _get_connection()
    cur = conn.cursor()

    # テーブル作成を念のため確認
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT PRIMARY KEY,
            item TEXT NOT NULL,
            quantity INT NOT NULL
        )
    """)

    for data in data_list:
        cur.execute(
            "INSERT INTO orders (id, item, quantity) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (data["id"], data["item"], data["quantity"])
        )

    conn.commit()
    cur.close()
    conn.close()
    return {"message": f"{len(data_list)} records inserted into Postgres"}

def get_user():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SELECT name FROM users LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return {"name": row[0]} if row else {"name": "Unknown"}