# Projeto: Crypto Market Overview - Enterprise Data Pipeline
> **Pipeline de dados para análise de criptomoedas near real-time utilizando arquitetura cloud-native(Google Cloud Platform)**
<p align="center">
  <img  src="imagens/pipeline.png">
  <b>Fluxo de processo</b>
</p>


### 🏆 Principais Características

- **🔄 Pipeline Event-Driven**: Orquestração via Airflow com triggers Pub/Sub
- **☁️ Cloud-Native**: 100% serverless utilizando Google Cloud Platform
- **🏗️ Arquitetura Medalion**: Separação clara de responsabilidades por camadas
- **📊 Analytics Avançadas**: 4 análises estratégicas automatizadas
- **🔍 Near Real-Time Insights**: Dashboard interativo com dados atualizados
- **🛡️ Data Quality**: Transactions com Delta Lake para consistência de dados
- **📈 Escalabilidade**: Processamento distribuído com Dataproc Serverless e Apache Spark

### 🔧 Componentes da Arquitetura

| Componente | Tecnologia | Função |
|------------|------------|--------|
| **Infraestrutura Cloud** | Google Cloud Platform| Plataforma principal de infraestrutura |
| **Linguagem principal** | Python| Principal linguagem de programação | 
| **Orquestração** | Composer - Apache Airflow | Agendamento e monitoramento da pipeline |
| **Messaging** | Google Pub/Sub | Event-driven triggers para Cloud Run |
| **API Gateway** | Cloud Run | Extração serverless de dados da API |
| **Data Lake** | Cloud Storage | Armazenamento de dados RAW (Bronze) |
| **Processing** | Dataproc Serverless | Transformações distribuídas com Spark |
| **Delta Lake** | Cloud Storage | Versionamento e transações ACID |
| **Data Warehouse** | BigQuery | Consultas analíticas de alta performance |
| **Visualization** | Looker Studio | Dashboards interativos e insights |


## 🎯 Visão Geral do Projeto

O **Crypto Market Overview** é uma solução completa de Data Engineering que consome dados da [API CoinCap](https://docs.coincap.io/) para gerar insights estratégicos sobre o mercado de criptomoedas. O projeto implementa uma **arquitetura medalion** (Bronze → Silver → Gold) utilizando tecnologias cloud-native para garantir escalabilidade, confiabilidade e performance.


## 🏗️ Arquitetura da Solução

### 📂 Códigos do Projeto
- 🔗 [Cloud Run - Extração API](1-cloud_function_get_data.py)
- 🔗 [PySpark Bronze→Silver](2-spark_processed_assats_list.py) 
- 🔗 [PySpark Silver→Gold](3-spark_curated_analytics_full.py)
- 🔗 [Airflow DAG Principal](4-dag_assats-list-full.py)

---

### 🔄 Orquestração com Airflow
O Apache Airflow gerencia toda a execução da pipeline, desde o trigger inicial até a finalização das análises.

<p align="center">
  <img src="imagens/airflow_dag.png">
  <br><b>Fluxo de execução da DAG</b>
</p>

### ☁️ Extração Serverless
O Cloud Run processa eventos do Pub/Sub e extrai dados da API CoinCap de forma completamente serverless.
Alimenta camada Bronze.

<p align="center">
  <img src="imagens/cloud_function.png">
  <br><b>Execução do Cloud Run</b>
</p>

### ⚡ Processamento Distribuído
O Dataproc Serverless executa jobs PySpark para criação de camadas Silver→Gold sem necessidade de gerenciar clusters.

<p align="center">
  <img src="imagens/jobs_dataproc_serveless.png">
  <br><b>Execução dos Jobs PySpark com Dataproc Serverless</b>
</p>

---

## 📊 Camadas de Dados (Medalion Architecture)

### 🥉 Bronze Layer - Raw Data
- **Fonte**: API CoinCap REST v3
- **Formato**: JSON files com timestamp único
- **Localização**: `gs://coincap/raw/assets_list/`
- **Atualização**: Event-driven via Pub/Sub
- **Retenção**: Histórico completo para auditoria

### 🥈 Silver Layer - Processed Data  
- **Processamento**: Limpeza, tipagem e estruturação
- **Formato**: Delta Tables particionadas
- **Localização**: `gs://coincap/processed/assets_list/`
- **Features**: Schema evolution, time travel, ACID transactions
- **Qualidade**: Validações de integridade e consistência

### 🥇 Gold Layer - Analytics Ready
- **Análises**: 4 visões estratégicas automatizadas
- **Formato**: Delta Tables + BigQuery Tables
- **Localização**: `gs://coincap/curated/{name_analytics}/` 
- **Outputs**: Datasets otimizados para consumo analítico

---

## 📈 Análises Implementadas

### 🔍 Estrutura das Tabelas BigQuery
| Análise | Descrição |
|---------|-----------|
| **📊 daily_overview** | Snapshot completo do mercado atual |
| **📈 top_gainers_losers** | Variações do dia |
| **🏆 market_dominance** | Participação no mercado total |
| **⚖️ supply_dynamics** | Análise de escassez e oferta |

### 📋 Query Unificada para Dashboard
- 🔗 [SQL - Query Master para Looker Studio](5-sql_exportacao_de_dados_dashboard.sql)

O BigQuery atua como data warehouse principal, oferecendo consultas analíticas de alta performance para alimentar os dashboards.

<p align="center">
  <img src="imagens/bigquery.png">
  <br><b>Ambiente de consultas analíticas no BigQuery</b>
</p>



## 📊 Dashboard Final
Dashboard interativo com métricas carregadas em near real-time, análises de mercado e insights estratégicos de criptomoedas.

<p align="center">
  <img src="imagens/dash_board.jpg">
  <br><b>Dashboard Looker Studio com análises em tempo real</b>
</p>

[Click e acesse a dashboard](https://lookerstudio.google.com/reporting/067747b1-5ffb-4076-ba08-aca53809d5f4) 

---

## 🎯 Conclusão

Este projeto demonstra a implementação de uma **pipeline de dados enterprise** completa, aplicando as melhores práticas de **Data Engineering** moderno. A solução combina tecnologias de ponta como **Apache Spark**, **Delta Lake** e **arquitetura serverless** para criar um sistema robusto, escalável e eficiente.

### 🏆 Principais Conquistas Técnicas:

- **✅ Arquitetura Cloud-Native**: 100% serverless com auto-scaling e pay-per-use
- **✅ Medalion Architecture**: Separação clara de responsabilidades por camadas
- **✅ Event-Driven Design**: Pipeline reativa e eficiente com Pub/Sub
- **✅ ACID Compliance**: Consistência de dados garantida com Delta Lake
- **✅ Analytics Automatizadas**: 4 análises estratégicas geradas automaticamente
- **✅ Observabilidade**: Monitoramento completo via Cloud Logging e Airflow

### 📈 Valor de Negócio:

O sistema entrega **insights acionáveis** sobre o mercado de criptomoedas através de um dashboard interativo, permitindo análises de dominância de mercado, identificação de oportunidades (gainers/losers) e avaliação de dinâmicas de oferta e demanda. A arquitetura garante que os dados estejam sempre atualizados e confiáveis para tomada de decisões estratégicas.

### 🚀 Impacto Técnico:

Esta implementação serve como **referência** para pipelines de dados modernas, demonstrando como integrar múltiplas tecnologias cloud para criar soluções empresariais robustas. O projeto evidencia conhecimento profundo em **Data Engineering**, **Cloud Architecture** e **Analytics**, aplicando padrões de mercado utilizados por empresas de tecnologia de grande porte.
