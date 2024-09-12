from getdata import obter_todos_os_dados, transformar_dados_recursos
from utils import salvar_dados_no_sql, conectar_sql_server

def verificar_ou_criar_tabelas():
    conexao = conectar_sql_server()
    cursor = conexao.cursor()

    # Truncar a tabela 'EscolasAtendidas' se ela existir
    cursor.execute("""
        IF OBJECT_ID('EscolasAtendidas', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE EscolasAtendidas
        END
    """)

    # Verifica se a tabela 'EscolasAtendidas' existe e a cria caso não exista
    cursor.execute("""
        IF OBJECT_ID('EscolasAtendidas', 'U') IS NULL
        BEGIN
            CREATE TABLE EscolasAtendidas (
                CodEscolasAtendidas INT,
                Ano VARCHAR(4),
                UF VARCHAR(2),
                Municipio VARCHAR(60),
                EsferaGoverno VARCHAR(60),
                QtdEscolasAtendidas INT
            )
        END
    """)

    # Truncar a tabela 'Repasse' se ela existir
    # cursor.execute("""
    #     IF OBJECT_ID('Repasse', 'U') IS NOT NULL
    #     BEGIN
    #         TRUNCATE TABLE Repasse
    #     END
    # """)

    # # Verifica se a tabela 'Repasse' existe e a cria caso não exista
    # cursor.execute("""
    #     IF OBJECT_ID('Repasse', 'U') IS NULL
    #     BEGIN
    #         CREATE TABLE Repasse (
    #             Co_recursos_repassados INT,
    #             Ano VARCHAR(255),
    #             Estado VARCHAR(255),
    #             Municipio VARCHAR(255),
    #             Esfera_governo VARCHAR(255),
    #             Modalidade_ensino VARCHAR(255),
    #             Vl_total_escolas FLOAT
    #         )
    #     END
    # """)

    conexao.commit()
    cursor.close()
    conexao.close()
    print("Tabelas criadas ou truncadas com sucesso")


url_escolas = "https://www.fnde.gov.br/olinda-ide/servico/PDA_Escolas_Atendidas/versao/v1/odata/EscolasAtendidas?$top=100&$format=json&$select=CodEscolasAtendidas,Ano,UF,Municipio,EsferaGoverno,QtdEscolasAtendidas"
# url_repasse = "https://www.fnde.gov.br/olinda-ide/servico/PNAE_Recursos_Repassados_Pck_3/versao/v1/odata/RecursosRepassados?$top=100&$format=json&$select=Co_recursos_repassados,Ano,Estado,Municipio,Esfera_governo,Modalidade_ensino,Vl_total_escolas"

if __name__ == "__main__":
    # Verifica e cria as tabelas se necessário
    verificar_ou_criar_tabelas()

    # Obter os dados das escolas atendidas e do repasse
    df_escolas = obter_todos_os_dados(url_escolas)
    # df_repasse = obter_todos_os_dados(url_repasse)

    if df_escolas is not None:
        # Salvar os dados no SQL Server
        salvar_dados_no_sql(df_escolas, 'EscolasAtendidas2')
        print("Dados de escolas salvos com sucesso")

    # if df_repasse is not None:
    #     
    #     df_repasse = transformar_dados_recursos(df_repasse)

    #     # Verificar se há valores que ainda não são válidos para FLOAT
    #     invalid_values = df_repasse[df_repasse['Vl_total_escolas'].apply(lambda x: not isinstance(x, float) and x is not None)]

    #     print("Valores com problema'Vl_total_escolas':")
    #     print(invalid_values)

    #     salvar_dados_no_sql(df_repasse, 'Repasse')
    #     print("Dados de repasse salvos com sucesso")
