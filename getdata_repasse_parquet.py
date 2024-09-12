import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


# Obter dados api
def fetch_data(skip):
    url = f"https://www.fnde.gov.br/olinda-ide/servico/PNAE_Recursos_Repassados_Pck_3/versao/v1/odata/RecursosRepassados?$top=100&$skip={skip}&$format=json&$select=Co_recursos_repassados,Ano,Estado,Municipio,Esfera_governo,Modalidade_ensino,Vl_total_escolas"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'value' in data and data['value']:
            return pd.DataFrame(data['value'])  
        else:
            return None  
    else:
        return None
# Salva os dados em suas respectivas pastas com o nome do estado de forma sequencial
def save_data(data, part):
    if data is not None and not data.empty:
        grouped = data.groupby('Estado')
        for state, group in grouped:
            state_folder = f'data/{state}'
            if not os.path.exists(state_folder):
                os.makedirs(state_folder, exist_ok=True)
            file_path = f'{state_folder}/recursos_repassados_part_{part}.parquet'
            group.to_parquet(file_path, index=False)
            print(f'Dados salvos em {file_path}')

# Utiliza 10 threads para carregar e salvar os dados de forma paralela, salvando em arquivos com 100 linhas.
def parallel_fetch_and_save():
    skip = 0
    more_data = True
    part = 0

    while more_data:
        future_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:

            for _ in range(10):
                future_data.append(executor.submit(fetch_data, skip))
                skip += 100  


            for future in as_completed(future_data):
                df = future.result()
                if df is not None and not df.empty:
                    save_data(df, part)
                    part += 1
                else:
                    more_data = False  

if __name__ == "__main__":
    parallel_fetch_and_save()
