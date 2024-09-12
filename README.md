# Programa Nacional de Alimentação Escolar (PNAE)


Este projeto foi desenvolvido para resolver problemas críticos enfrentados pela Secretaria do Estado de Mato Grosso do Sul - SED/MS na gestão do Programa Nacional de Alimentação Escolar (PNAE). Os principais desafios identificados incluem a demora na obtenção de dados, prazos não atendidos, e a inconsistência das informações, o que impacta negativamente na tomada de decisões e pode resultar em multas.


**Tecnologias Utilizadas**

**Python:** Utilizado para automatizar a ingestão de dados e processar grandes volumes de informações.

**BigQuery:** Plataforma de armazenamento e consulta de dados em nuvem.

**Airflow:** Planejado para rodar pipelines em paralelo, possibilitando a ingestão de dados de todos os estados simultaneamente.

**SQL Server:** Banco de dados utilizado para armazenar os dados extraídos e permitir consultas personalizadas via SQL.

**Power BI:** Ferramenta de visualização de dados utilizada para criar dashboards interativos e apresentar as respostas aos questionamentos levantados no case.



**Tabela de Alunos:** Os dados foram recebidos em um arquivo CSV e inseridos no banco de dados. Foi convertido para parquet e inserido com duckdb no banco.

**Tabela de Repasses:** Dada a grande quantidade de dados, adotei uma abordagem de particionamento por estado. Os dados foram extraídos de uma API em lotes de 100 registros, armazenados localmente para garantir a integridade e continuidade do processo em caso de falhas. Esse método de particionamento permitiu a ingestão paralela dos dados, resultando em uma melhoria significativa na velocidade de processamento.

**Tabela de Escolas Atendidas:** Os dados foram extraídos diretamente de um arquivo e carregados no banco de dados.


**Processamento e Armazenamento**
Os dados processados foram carregados em um banco de dados SQL Server e, também realizei o upload do output do script getdata_repasse_parquet.py em bucket na gcp por conta do seu tamanho 1,2(GB)


**Análise de Performance**
***Melhoria de 12% no Tempo de Ingestão:** Mesmo no pior cenário, o tempo de ingestão dos dados foi reduzido em 12%, de 4 para 3,5 dias, representando uma melhoria significativa no processo.

**Ingestão Paralela:** A tabela mais pesada (estado de MG) foi processada em 48h. Com a implementação do Airflow, seria possível reduzir em até 50% o processamento desses


**Foco na Performance**
Durante todo o desenvolvimento do projeto, o foco principal foi maximizar a performance da ingestão de dados. Isso foi alcançado através do uso de uma máquina virtual para executar os scripts, juntamente com o particionamento dos dados e a execução paralela das tarefas, garantindo uma ingestão eficiente e escalável.
