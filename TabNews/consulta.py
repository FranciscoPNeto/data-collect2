# %%
import sqlite3
import pandas as pd

#conectar no banco
conn = sqlite3.connect("data/contents/sqlite/tabnews.db")

#ler a tabela
df = pd.read_sql("SELECT * FROM contents LIMIT 10;", conn)  #mostra os 10 primeiros
print(df)

conn.close()
