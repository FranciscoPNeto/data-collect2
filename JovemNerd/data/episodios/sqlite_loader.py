import sqlite3
import json
import os
from datetime import datetime

def criar_tabela_sqlite():
    os.makedirs("sqlite", exist_ok=True)
    conn = sqlite3.connect("sqlite/nerdcast.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episodios (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            link TEXT,
            published_at TEXT,
            duration TEXT,
            explicit INTEGER,
            ingestion_date TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hosts (
            episodio_id INTEGER,
            host_name TEXT,
            FOREIGN KEY(episodio_id) REFERENCES episodios(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS guests (
            episodio_id INTEGER,
            guest_name TEXT,
            FOREIGN KEY(episodio_id) REFERENCES episodios(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            episodio_id INTEGER,
            category_name TEXT,
            FOREIGN KEY(episodio_id) REFERENCES episodios(id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Banco criado: sqlite/nerdcast.db")

def inserir_dados_sqlite(caminho_json):
    with open(caminho_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = sqlite3.connect("sqlite/nerdcast.db")
    cursor = conn.cursor()

    # Extrai data do nome do arquivo: 20250101_120000_123456.json
    nome = os.path.basename(caminho_json)
    try:
        ano, mes, dia = nome[:4], nome[4:6], nome[6:8]
        ingestion_date = f"{ano}-{mes}-{dia}"
    except:
        ingestion_date = datetime.now().strftime("%Y-%m-%d")

    for ep in data:
        cursor.execute('''
            INSERT OR IGNORE INTO episodios 
            (id, title, link, published_at, duration, explicit, ingestion_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            ep.get('id'),
            ep.get('title'),
            ep.get('link'),
            ep.get('published_at'),
            ep.get('duration'),
            int(ep.get('explicit', False)),
            ingestion_date
        ))

        ep_id = ep.get('id')

        for host in ep.get('hosts', []):
            cursor.execute('INSERT OR IGNORE INTO hosts VALUES (?, ?)', (ep_id, host))

        for guest in ep.get('guests', []):
            cursor.execute('INSERT OR IGNORE INTO guests VALUES (?, ?)', (ep_id, guest))

        for cat in ep.get('categories', []):
            cursor.execute('INSERT OR IGNORE INTO categories VALUES (?, ?)', (ep_id, cat))

    conn.commit()
    conn.close()
    print(f"Importado: {nome} → {len(data)} episódios")