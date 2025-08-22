# Tech_Challenge_Bees
Repositório com os pipelines referentes ao Breweries Case

O projeto Open Brewery Medallion implementa um pipeline de dados baseado no modelo de arquitetura Medallion (Bronze, Silver e Gold). Ele consome dados da API pública Open Brewery (https://www.openbrewerydb.org/), realiza processamento e aplica validações de qualidade antes de armazenar os dados em camadas organizadas.

O Open Brewery DB é um conjunto de dados e API gratuitos com informações públicas sobre cervejarias, sidrerias, cervejarias artesanais e lojas de bebidas de diversos países do mundo.

Neste projeto, o pipeline é orquestrado pelo Apache Airflow, com transformações em PySpark e persistência em formatos otimizados para análise, como json e parquet. Os testes são realizados com a biblioteca pytest.
