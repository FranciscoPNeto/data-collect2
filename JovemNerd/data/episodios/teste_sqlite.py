from sqlite_loader import criar_tabela_sqlite, inserir_dados_sqlite
import glob
import os

print("INICIANDO IMPORTAÇÃO PARA SQLITE...\n")

criar_tabela_sqlite()

arquivos = glob.glob("json/*.json")
if not arquivos:
    print("Nenhum JSON encontrado em /json/")
else:
    print(f"Encontrados {len(arquivos)} arquivos JSON\n")
    for arq in sorted(arquivos):
        inserir_dados_sqlite(arq)

print("\nCONCLUÍDO!")
print("Banco final: sqlite/nerdcast.db")