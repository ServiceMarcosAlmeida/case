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
Inserção eficiente no banco de dados usando SQLServer.

**2. ingestao_repasses.py**

Descrição:
Este script lida com a ingestão de dados da tabela de repasses, que é uma das mais volumosas. A abordagem adotada foi particionar os dados por estado, realizando chamadas à API em lotes de 100 registros para acelerar o processamento.

_Pontos Técnicos_: 
Uso de requisições HTTP para interagir com a API.
Particionamento por estado para paralelização e otimização do processo.
Salvamento dos dados em pastas organizadas, permitindo a retomada do processo em caso de falhas.
Inserção no banco de dados e particionamento em BigQuery para análise posterior.

**3. ingestao_escolas.py**

Descrição:
Este script é responsável por ler os dados das escolas atendidas, processá-los e salvá-los diretamente no banco de dados. Como essa tabela é menor em comparação com a de repasses, o foco foi na simplicidade e eficiência do processo de ingestão.

_Pontos Técnicos_: 
Processamento direto de arquivos CSV.
Inserção no banco de dados SQL Server.

**4. utils.py** 

Descrição:
Esse script contém funções genéricas.

_Pontos Técnicos_: 
Usada para reaproveitamento de código.


# Processamento e Armazenamento

Os dados processados foram armazenados em um banco de dados SQL Server e, para demonstração, também foram inseridos no BigQuery.

# Análise de Performance

**Melhoria de 12% no Tempo de Ingestão:** O tempo de ingestão dos dados foi reduzido de 4 para 3,5 dias no pior cenário, representando uma melhoria significativa.

**Ingestão Paralela:** A tabela mais pesada (estado de MG) foi processada em 11 horas. Com o uso de Airflow, seria possível reduzir esse tempo significativamente ao processar dados de múltiplos estados simultaneamente.

![image](https://github.com/user-attachments/assets/ff70d188-d46d-4b19-8cbb-22b7dba9d650)





