# %%
import os
import sqlite3
import pandas as pd

def criar_tabela_sqlite(db_path="data/sqlite/resident_evil.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        link TEXT,
        aparicoes TEXT,
        info_json TEXT
    )
    """)
    
    conn.commit()
    conn.close()

def inserir_dados_sqlite(df, db_path="data/sqlite/resident_evil.db"):
    conn = sqlite3.connect(db_path)
    # Converte listas/dicionários em strings JSON antes de inserir
    df = df.copy()
    if "Aparicoes" in df.columns:
        df["Aparicoes"] = df["Aparicoes"].apply(lambda x: str(x))
    df["info_json"] = df.apply(lambda row: row.to_json(), axis=1)
    df[["Nome", "link", "Aparicoes", "info_json"]].to_sql(
        "personagens", conn, if_exists="append", index=False
    )
    conn.close()

# %%
