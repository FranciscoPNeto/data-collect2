# %%

import requests
import datetime
import json
import os

class Collector:

    def __init__(self, url, instance_name=None):
        self.url = url
        if instance_name is None:
            self.instance = url.strip("/").split("/")[-1]
        else:
            self.instance = instance_name
        os.makedirs(f"data/pokemon/{self.instance}", exist_ok=True)

    def get_endpoint(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp

    def save_data(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        data['ingestion_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filename = f"data/pokemon/{self.instance}/{now}.json"
        with open(filename, "w", encoding="utf-8") as open_file:
            json.dump(data, open_file, indent=2, ensure_ascii=False)
            
    def get_and_save(self, **kwargs):
        resp = self.get_endpoint(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            self.save_data(data)
            return data
        else:
            print(f"Erro {resp.status_code}: {resp.text}")
            return {}

    def auto_exec(self, limit=100):
        offset = 0
        while True:
            print(offset)
            data = self.get_and_save(limit=limit, offset=offset)
            if data.get("next") is None:
                break
            offset += limit

# %%

url = "https://pokeapi.co/api/v2/pokemon"
collector = Collector(url)
collector.auto_exec()

# %%

url = "https://pokeapi.co/api/v2/pokemon"
resp = requests.get(url)
resp.json()

def save_pokemon(data):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    data['data_ingestion'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename = "data/pokemon.json"
    with open(filename, "w", encoding="utf-8") as open_file:
        json.dump(data, open_file, indent=2, ensure_ascii=False)

def get_and_save(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        save_pokemon(data)
    else:
        print("Não foi possível!!")        
# %%
get_and_save("https://pokeapi.co/api/v2/pokemon/1/")
# %%