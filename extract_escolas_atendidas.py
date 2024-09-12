import requests
import pandas as pd
import numpy as np
from utils import save_data_sql

## Caso queira salvar mais dados ajuste o limite
def obter_todos_os_dados_e_inserir(url_base, tabela, limit=100):
    offset = 0
    while True:
        url = f"{url_base}&$top={limit}&$skip={offset}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Isso vai levantar um erro para códigos de status 4xx/5xx
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  
            break
        except Exception as err:
            print(f"Ocorreu outro erro: {err}")  # Exibe qualquer outro erro
            break
        
        dados = response.json()
        
        if 'value' in dados:
            df = pd.json_normalize(dados['value'])
            
            if df.empty:
                print("Nenhum dado retornado. Interrompendo a coleta.")
                break
            
            # Inserir os dados no banco de dados
            salvar_dados_no_sql(df, tabela)
            print(f"{len(df)} registros inseridos na tabela {tabela}.")
            
            # Sem dados novos
            if len(df) < limit:
                print("Todos os dados foram coletados e inseridos.")
                break
        else:
            print("Não encontrado no JSON.")
            break
        
        offset += limit
        print(f"Progredindo para a próxima página, offset atual: {offset}")


def transformar_dados_recursos(df):
    tipos = {
        'Co_recursos_repassados': 'int',
        'Ano': 'str',
        'Estado': 'str',
        'Municipio': 'str',
        'Esfera_governo': 'str',
        'Modalidade_ensino': 'str',
        'Vl_total_escolas': 'float'
    }

    
    df['Vl_total_escolas'] = df['Vl_total_escolas'].str.replace(',', '.').astype(float)
  
    print("Tipos de dados na coluna 'Vl_total_escolas' após correção:", df['Vl_total_escolas'].apply(type).unique())
    
    df = df.astype(tipos)    
    return df





url_escolas = "https://www.fnde.gov.br/olinda-ide/servico/PDA_Escolas_Atendidas/versao/v1/odata/EscolasAtendidas?$format=json&$select=CodEscolasAtendidas,Ano,UF,Municipio,EsferaGoverno,QtdEscolasAtendidas"


obter_todos_os_dados_e_inserir(url_escolas, 'EscolasAtendidas')
