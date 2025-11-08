# %%
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
}

# Corrigida a função get_content
def get_content(url):
    resp = requests.get(url, headers=headers)
    return resp

def get_basic_infos(soup):
    div_page = soup.find("div", class_="td-page-content")
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
        print("Não foi possível exibir a página!")
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    data = get_basic_infos(soup)
    data["Aparicoes"] = get_aparicoes(soup)
    return data

def get_links():
    url = "https://www.residentevildatabase.com/personagens/"
    resp = requests.get(url, headers=headers)
    soup_personagens = BeautifulSoup(resp.text, "html.parser")
    div_content = soup_personagens.find("div", class_="td-page-content")
    if not div_content:
        return []
    ancoras = div_content.find_all("a")
    links = [i["href"] for i in ancoras if i.get("href")]
    return links

# %%
links = get_links()
data = []
for i in tqdm(links):
    d = get_personagem_info(i)
    d["link"] = i
    nome = i.split("/")[-1].replace("-", " ").title()
    d["Nome"] = nome
    data.append(d)

# %%
df = pd.DataFrame(data)
df

# %%

df.to_csv("Dados_re.csv", index=False, sep=";")
# %%

df.to_parquet("dados_re.parquet", index=False)

# %%
