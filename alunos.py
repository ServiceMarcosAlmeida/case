import os
import pandas as pd
import pyodbc
import duckdb
import logging
from config import DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD

## adicionei um log para validar quanto tempo iria demorar para inserir

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

csv_file_path = 'Alunos_atendidos.csv'
parquet_file_path = 'Alunos_atendidos.parquet'

# Converter CSV para Parquet
logger.info(f"Convertendo '{csv_file_path}' para '{parquet_file_path}'...")
df = pd.read_csv(csv_file_path)
df.to_parquet(parquet_file_path, index=False)
logger.info(f"Arquivo convertido com sucesso para '{parquet_file_path}'.")

# Construir a string de conexão usando variáveis de config.py
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_HOST};DATABASE={DB_DATABASE};UID={DB_USER};PWD={DB_PASSWORD};"


logger.info("Conectando ao banco de dados SQL Server...")
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Verificar se a tabela já existe e criar se necessário
check_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='alunos_pnae' AND xtype='U')
BEGIN
    CREATE TABLE alunos_pnae (
        Co_alunos_atendidos INTEGER,
        Ano INTEGER,
        Estado VARCHAR(50),
        Municipio VARCHAR(100),
        Regiao VARCHAR(50),
        Esfera_governo VARCHAR(50),
        Etapa_ensino VARCHAR(100),
        Qt_alunos_pnae INTEGER
    );
END
"""
logger.info("Verificando a existência da tabela 'alunos_pnae' e criando se necessário...")
cursor.execute(check_table_query)
conn.commit()


logger.info(f"Carregando dados do arquivo Parquet: {parquet_file_path}")
df_parquet = duckdb.query(f"SELECT * FROM '{parquet_file_path}'").df()

# Defina o tamanho do lote para inserção em massa
batch_size = 1000
rows = []

# Inserir os dados em lote
logger.info("Iniciando a inserção dos dados em lote...")

for index, row in df_parquet.iterrows():
    rows.append(tuple(row))
    

    if len(rows) == batch_size:
        cursor.executemany("""
            INSERT INTO alunos_pnae (Co_alunos_atendidos, Ano, Estado, Municipio, Regiao, Esfera_governo, Etapa_ensino, Qt_alunos_pnae)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        rows = []  # Limpa a lista de rows após a inserção


if rows:
    cursor.executemany("""
        INSERT INTO alunos_pnae (Co_alunos_atendidos, Ano, Estado, Municipio, Regiao, Esfera_governo, Etapa_ensino, Qt_alunos_pnae)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

logger.info("Fechando a conexão com o banco de dados.")
cursor.close()
conn.close()

logger.info("Processo concluído com sucesso.")
