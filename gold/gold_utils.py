import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASS = "sa11%4022"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "adventureworks"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    future=True
)

def load_df(query):
    return pd.read_sql(query, engine)

def write_table(df, name):
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{name}" CASCADE;'))
    df.to_sql(name, engine, index=False)
    print(f"[ ✔ ] Created GOLD TABLE → {name}")

def create_view(view_name, query):
    with engine.begin() as conn:
        conn.execute(text(f'DROP VIEW IF EXISTS "{view_name}" CASCADE;'))
        conn.execute(text(f'CREATE VIEW "{view_name}" AS {query}'))
    print(f"[ ✔ ] Created VIEW → {view_name}")
