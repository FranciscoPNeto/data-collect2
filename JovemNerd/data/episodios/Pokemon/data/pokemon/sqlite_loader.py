# %%
import sqlite3
import json
import os
from pathlib import Path

# Usa a pasta atual (onde o script está rodando)
BASE_DIR = Path.cwd()
SQLITE_DIR = BASE_DIR / "sqlite"
DB_PATH = SQLITE_DIR / "pokemon.db"

def criar_tabela_sqlite():
    """
    Cria a pasta 'sqlite' e o banco com as tabelas.
    Rode apenas uma vez.
    """
    SQLITE_DIR.mkdir(exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabela principal
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        url TEXT,
        ingestion_date TEXT,
        height INTEGER,
        weight INTEGER,
        base_experience INTEGER,
        is_default BOOLEAN,
        order_num INTEGER
    )
    ''')

    # Tipos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon_types (
        pokemon_id INTEGER,
        slot INTEGER,
        type_name TEXT,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon (id)
    )
    ''')

    # Habilidades
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon_abilities (
        pokemon_id INTEGER,
        slot INTEGER,
        ability_name TEXT,
        is_hidden BOOLEAN,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon (id)
    )
    ''')

    # Stats
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon_stats (
        pokemon_id INTEGER,
        stat_name TEXT,
        base_stat INTEGER,
        effort INTEGER,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon (id)
    )
    ''')

    conn.commit()
    conn.close()
    print(f"[OK] Banco criado em: {DB_PATH}")
    print("     → Use DB Browser for SQLite ou VS Code para abrir!")


def inserir_dados_sqlite(caminho_json: str):
    """
    Lê um JSON (lista ou detalhe) e insere no banco.
    """
    if not os.path.exists(caminho_json):
        print(f"[ERRO] Arquivo não encontrado: {caminho_json}")
        return

    with open(caminho_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # === LISTA PAGINADA ===
    if "results" in data:
        for item in data["results"]:
            cursor.execute('''
            INSERT OR IGNORE INTO pokemon (name, url, ingestion_date)
            VALUES (?, ?, ?)
            ''', (item["name"], item["url"], data.get("ingestion_date")))

    # === DETALHE INDIVIDUAL ===
    elif "id" in data and "name" in data:
        p = data
        cursor.execute('''
        INSERT OR REPLACE INTO pokemon 
        (id, name, url, ingestion_date, height, weight, base_experience, is_default, order_num)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            p["id"], p["name"],
            p.get("url") or f"https://pokeapi.co/api/v2/pokemon/{p['id']}/",
            p.get("ingestion_date"),
            p.get("height"), p.get("weight"), p.get("base_experience"),
            p.get("is_default"), p.get("order")
        ))

        # Tipos
        for slot, t in enumerate(p.get("types", []), 1):
            cursor.execute('INSERT INTO pokemon_types VALUES (?, ?, ?)', 
                         (p["id"], slot, t["type"]["name"]))

        # Habilidades
        for slot, a in enumerate(p.get("abilities", []), 1):
            cursor.execute('INSERT INTO pokemon_abilities VALUES (?, ?, ?, ?)', 
                         (p["id"], slot, a["ability"]["name"], a["is_hidden"]))

        # Stats
        for s in p.get("stats", []):
            cursor.execute('INSERT INTO pokemon_stats VALUES (?, ?, ?, ?)', 
                         (p["id"], s["stat"]["name"], s["base_stat"], s["effort"]))

    conn.commit()
    conn.close()
    print(f"[OK] Inserido: {os.path.basename(caminho_json)}")
# %%
