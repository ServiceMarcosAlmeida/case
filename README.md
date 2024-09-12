# Programa Nacional de Alimentação Escolar (PNAE)


Este projeto foi desenvolvido para resolver problemas críticos enfrentados pela Secretaria do Estado de Mato Grosso do Sul - SED/MS na gestão do Programa Nacional de Alimentação Escolar (PNAE). Os principais desafios identificados incluem a demora na obtenção de dados, prazos não atendidos, e a inconsistência das informações, o que impacta negativamente na tomada de decisões e pode resultar em multas.
A solução envolve a ingestão de dados de várias fontes, armazenamento em diferentes bancos de dados, e a criação de dashboards interativos no Power BI para análise e tomada de decisões.

# Caracteristicas & Tecnologias Utilizadas

**Python:** Utilizado para automatizar a ingestão de dados e processar grandes volumes de informações.

**BigQuery:** Plataforma de armazenamento e consulta de dados em nuvem.

**Airflow:** Planejado para rodar pipelines em paralelo, possibilitando a ingestão de dados de todos os estados simultaneamente.

**SQL Server:** Banco de dados utilizado para armazenar os dados extraídos e permitir consultas personalizadas via SQL.

**Power BI:** Ferramenta de visualização de dados utilizada para criar dashboards interativos e apresentar as respostas aos questionamentos levantados no case.



# Estrutura do Projeto
**1. ingestao_alunos**

Descrição:Este script é responsável por ler o arquivo CSV contendo dados dos alunos e inseri-los no banco de dados SQL Server. O script foi projetado para processar e validar os dados antes da inserção, garantindo a integridade dos mesmos

_Pontos Técnicos_: 

Leitura e processamento do CSV usando Pandas.
Validação de dados para evitar inconsistências na base.
Inserção eficiente no banco de dados usando SQLAlchemy.
