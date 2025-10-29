# %%
import requests
import pandas as pd
import datetime
import json
import os
import time  # faltava importar o módulo time

# %%
def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp

def save_data(data, option='json'):
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")

    if option == 'json':
        os.makedirs("data/contents/json", exist_ok=True)
        with open(f"data/contents/json/{now}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    elif option == 'parquet':
        os.makedirs("data/contents/parquet", exist_ok=True)
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)

# %%
page = 1
while True:
    print(page)
    resp = get_response(page=page, per_page=100, strategy="new")
    if resp.status_code == 200:
        data = resp.json()  # faltava definir a variável data
        save_data(data)

        if len(data) < 100:
            break

        page += 1
        time.sleep(2)


    else:
        time.sleep(30)


# %%
