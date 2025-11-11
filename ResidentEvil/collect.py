# %%
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import sqlite3
import json
from datetime import datetime


HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
}

DB_PATH = "data/sqlite/resident_evil.db"


def get_content(url):
    resp = requests.get(url, headers=HEADERS)
    return resp

def get_basic_infos(soup):
    div_page = soup.find("div", class_="td-page-content")
    if not div_page:
        return {}
    paragraphs = div_page.find_all("p")
    if len(paragraphs) < 2:
        return {}
    paragrafo = paragraphs[1]
    ems = paragrafo.find_all("em")
    data = {}
    for i in ems:
        parts = i.text.split(":", 1)
        if len(parts) == 2:
            chave, valor = parts
            data[chave.strip()] = valor.strip()
    return data

def get_aparicoes(soup):
    div_page = soup.find("div", class_="td-page-content")
    h4_tag = div_page.find("h4")
    if not h4_tag:
        return []
    ul = h4_tag.find_next("ul")
    if not ul:
        return []
    lis = ul.find_all("li")
    aparicoes = [i.text.strip() for i in lis]
    return aparicoes

def get_personagem_info(url):
    resp = get_content(url)
    if resp.status_code != 200:
        print(f"Falha ao acessar: {url}")
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    data = get_basic_infos(soup)
    data["Aparicoes"] = get_aparicoes(soup)
    return data

def get_links():
    url = "https://www.residentevildatabase.com/personagens/"
    resp = requests.get(url, headers=HEADERS)
    soup_personagens = BeautifulSoup(resp.text, "html.parser")
    div_content = soup_personagens.find("div", class_="td-page-content")
    if not div_content:
        return []
    ancoras = div_content.find_all("a")
    links = [i["href"] for i in ancoras if i.get("href")]
    return links


def criar_tabela_sqlite(db_path=DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        link TEXT,
        aparicoes TEXT,
        info_json TEXT,
        ingestion_date TEXT
    )
    """)
    conn.commit()
    conn.close()

def inserir_dados_sqlite(df, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    df = df.copy()
    df["Aparicoes"] = df["Aparicoes"].apply(lambda x: json.dumps(x) if isinstance(x, list) else str(x))
    df["info_json"] = df.apply(lambda row: row.to_json(), axis=1)
    df["ingestion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df[["Nome", "link", "Aparicoes", "info_json", "ingestion_date"]].to_sql(
        "personagens", conn, if_exists="append", index=False
    )
    conn.close()


# PIPELINE PRINCIPAL
if __name__ == "__main__":
    print(" Coletando links de personagens...")
    links = get_links()
    print(f"Encontrados {len(links)} links.")

    data = []
    for i in tqdm(links, desc="Coletando personagens"):
        d = get_personagem_info(i)
        d["link"] = i
        nome = i.split("/")[-1].replace("-", " ").title()
        d["Nome"] = nome
        data.append(d)

    df = pd.DataFrame(data)
    print(f"Total de personagens coletados: {len(df)}")

    # Salvando arquivos locais
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/Dados_re.csv", index=False, sep=";")
    df.to_parquet("data/dados_re.parquet", index=False)
    print("Arquivos CSV e Parquet salvos em /data")

    # Salvando no SQLite
    criar_tabela_sqlite()
    inserir_dados_sqlite(df)
    print(f"Dados inseridos no banco SQLite: {DB_PATH}")

    print("Pipeline Resident Evil finalizado com sucesso!")
# %%
