# %%
from sqlite_loader import criar_tabela_sqlite, inserir_dados_sqlite
import glob
import os

print("INICIANDO TESTE DE IMPORTAÇÃO PARA SQLITE\n")

# 1. Criar banco
criar_tabela_sqlite()

# 2. Importar todos os JSONs
contador = 0
for arquivo in glob.glob("**/*.json", recursive=True):
    if "sqlite" not in arquivo.lower():  # evita loop
        inserir_dados_sqlite(arquivo)
        contador += 1

print(f"\n{contador} arquivos processados!")
print(f"Abra a pasta 'sqlite/pokemon.db' para ver o resultado!")
# %%
