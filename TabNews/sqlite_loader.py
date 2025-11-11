# %%
import pandas as pd
import sqlite3
import os
import json

#caminhos
json_folder = "data/contents/json"
db_path = "data/contents/sqlite/tabnews.db"
table_name = "contents"

#lista todos os arquivos JSON
json_files = [os.path.join(json_folder, f) for f in os.listdir(json_folder) if f.endswith(".json")]

#carrega todos os JSONs em um DataFrame
df_list = []
for file in json_files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        df_list.append(pd.DataFrame(data))

if not df_list:
    print("Nenhum arquivo JSON encontrado.")
    exit()

df = pd.concat(df_list, ignore_index=True)

#cria a pasta do banco, se não existir
os.makedirs(os.path.dirname(db_path), exist_ok=True)

#salva no SQLite (o banco será criado automaticamente)
conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, if_exists="replace", index=False)

#mostra confirmação
print(f"{len(df)} registros salvos na tabela '{table_name}' do banco '{db_path}'")

#consulta rápida para verificar
df_sql = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5;", conn)
print(df_sql)

conn.close()
# %%

