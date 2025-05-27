# PYSPARK JOB - SILVER TO GOLD LAYER (CURATED ANALYTICS)
# Processa dados estruturados (Silver) e gera análises estratégicas (Gold) para consumo de BI

# Importações necessárias para análises avançadas
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sha2, concat_ws, col, expr, row_number, trim, upper, when, explode, from_unixtime, sum, avg, round, percent_rank
from google.cloud import storage
import argparse
import sys
from delta import *
from google.cloud import bigquery
from pyspark.sql.window import Window

# Configuração do sistema de logging para monitoramento
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Inicialização da sessão Spark com configurações completas
logging.info("Inicializando SparkSession...")
spark = SparkSession.builder \
    .appName("CryptoAnalyticscuratedLayer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.1.jar") \
    .getOrCreate()
# Nome da aplicação para análises
# Extensões SQL Delta Lake
# Catálogo Delta
# Conector BigQuery

logging.info("SparkSession inicializada com sucesso.")

# Definição de variáveis temporais para controle de execução e auditoria
date_time_exec = datetime.now()
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S") # Ex: 20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S") # Ex: 2024-05-01 13:05:11

# Definição dos caminhos de entrada e saída das análises
# Caminho de entrada da camada Silver/Processed (onde está o df_assats_list)
input_path = f"gs://coincap/processed/assats_list"
# Caminho base de saída para a camada curated (onde as análises serão salvas)
output_curated_base_path = f"gs://coincap/curated/crypto_analytics"

# Configurações do BigQuery para persistência das análises
bigquery_project = "acoes-378306"
bigquery_dataset = "coincap"

# Carregamento dos dados da camada Silver (dados estruturados)
logging.info(f"Lendo dados da tabela Delta em: {input_path}")
try:
    df_assats_list = DeltaTable.forPath(spark, input_path).toDF()
    logging.info(f"DataFrame base 'df_assats_list' carregado com {df_assats_list.count()} registros.")
except Exception as e:
    logging.error(f"Erro ao carregar df_assats_list: {e}")
    sys.exit(1)
# Carrega Delta Table como DataFrame
# Encerra processo em caso de erro crítico

# Preparação dos dados para análises temporais
df_assats_list = df_assats_list.withColumn("data_referencia_dt", col("data_referencia").cast("timestamp")) # Garante tipo timestamp para séries temporais

# Definição de window function para obter registros mais recentes
# Define uma especificação de janela para particionar por 'id' e ordenar por 'data_referencia_dt' em ordem decrescente
# Isso nos permite pegar o registro mais recente para cada criptoativo
window_spec = Window.partitionBy("id").orderBy(col("data_referencia_dt").desc())

# Filtragem para dados mais atuais (snapshot current state)
# Obtém o registro mais recente para cada criptoativo com base em data_referencia_dt
df_latest_assats = df_assats_list \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")
# Adiciona número da linha (1 = mais recente)
# Filtra apenas registros mais recentes
# Remove coluna auxiliar

# Função auxiliar para persistência dual (Delta Lake + BigQuery)
def write_analysis_data(df, delta_output_path, bq_table_name):
    """
    Escreve o DataFrame para Delta (append) e BigQuery (overwrite).
    """
    # Persistência em Delta Lake (Gold Layer) - modo append para histórico
    logging.info(f"Escrevendo '{bq_table_name}' para Delta em: {delta_output_path} (modo append)")
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(delta_output_path)
    # Formato Delta Lake
    # Preserva histórico completo
    # Permite evolução de schema
    
    logging.info(f"Dados do Delta para '{bq_table_name}' salvos.")

    # Persistência em BigQuery - modo overwrite para análises atuais
    logging.info(f"Escrevendo '{bq_table_name}' para BigQuery em: {bigquery_project}.{bigquery_dataset}.{bq_table_name} (modo overwrite)")
    df.write.format("bigquery") \
        .option("temporaryGcsBucket", f"logs-apps/{bq_table_name}") \
        .option("project", 'acoes-378306') \
        .option("dataset", 'coincap') \
        .option("table", bq_table_name) \
        .mode("overwrite") \
        .save()
    # Bucket temporário para staging
    # Projeto GCP
    # Dataset BigQuery
    # Nome da tabela
    # Substitui dados anteriores
    
    logging.info(f"Dados do BigQuery para '{bq_table_name}' salvos.")
    
# ===== ANÁLISE 1: VISÃO GERAL DIÁRIA (DAILY SNAPSHOT) =====
# Snapshot completo do mercado atual com todas as métricas principais
logging.info("Iniciando Análise 1: Visão Geral Diária (Daily Snapshot)...")

df_daily_overview = df_latest_assats.select( # Use df_latest_assats aqui
    col("id"),
    col("name"),
    col("symbol"),
    col("rank"),
    round(col("price_usd"), 8).alias("price_usd"),
    round(col("market_cap_usd"), 2).alias("market_cap_usd"),
    round(col("volume_usd_24hr"), 2).alias("volume_usd_24hr"),
    round(col("change_percent_24hr"), 4).alias("change_percent_24hr"),
    round(col("vwap_24hr"), 8).alias("vwap_24hr"),
    round(col("supply"), 0).alias("supply"),
    round(col("max_supply"), 0).alias("max_supply"),
    col("explorer"),
    col("data_referencia_dt").alias("data_referencia")
).orderBy(col("rank").asc()) \
.withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp")) # Adiciona a data/hora de execução
# ID único da criptomoeda
# Nome completo
# Símbolo (BTC, ETH, etc.)
# Ranking por market cap
# Preço em USD (8 decimais para precisão)
# Market cap em USD
# Volume 24h em USD
# Variação percentual 24h
# Volume Weighted Average Price
# Oferta circulante
# Oferta máxima
# URL do blockchain explorer
# Data/hora de referência dos dados
# Ordena por ranking (melhor primeiro)

output_path_daily_overview = f"{output_curated_base_path}/daily_overview"
write_analysis_data(df_daily_overview, output_path_daily_overview, "daily_overview")
logging.info("Análise 1 concluída.")

# ===== ANÁLISE 2: TOP GANHADORES E PERDEDORES (24HR) =====
# Identifica as criptomoedas com as maiores e menores mudanças percentuais em 24 horas.
logging.info("Iniciando Análise 2: Top Ganhadores e Perdedores (24hr)...")

# Top 10 maiores altas (ganhadores)
df_gainers = df_latest_assats.filter(col("change_percent_24hr").isNotNull()) \
                            .orderBy(col("change_percent_24hr").desc()) \
                            .limit(10) \
                            .withColumn("tipo_movimento", lit("Ganhador"))
# Remove valores nulos
# Ordena por maior variação positiva
# Top 10 ganhadores
# Flag para identificar tipo

# Top 10 maiores baixas (perdedores)
df_losers = df_assats_list.filter(col("change_percent_24hr").isNotNull()) \
                           .orderBy(col("change_percent_24hr").asc()) \
                           .limit(10) \
                           .withColumn("tipo_movimento", lit("Perdedor"))
# Remove valores nulos
# Ordena por maior variação negativa
# Top 10 perdedores
# Flag para identificar tipo

# União dos datasets de ganhadores e perdedores
df_top_gainers_losers = df_gainers.unionAll(df_losers).select(
    col("name"),
    col("symbol"),
    round(col("change_percent_24hr"), 4).alias("change_percent_24hr"),
    round(col("price_usd"), 8).alias("price_usd"),
    col("tipo_movimento"),
    col("data_referencia_dt").alias("data_referencia")
) \
.withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp")) # Adiciona a data/hora de execução
# Nome da criptomoeda
# Símbolo
# Variação percentual
# Preço atual
# Ganhador ou Perdedor
# Data de referência

output_path_top_gainers_losers = f"{output_curated_base_path}/top_gainers_losers"
write_analysis_data(df_top_gainers_losers, output_path_top_gainers_losers, "top_gainers_losers")
logging.info("Análise 2 concluída.")


# ===== ANÁLISE 3: DOMINÂNCIA DE MERCADO POR CAPITALIZAÇÃO =====
# Calcula a participação percentual de cada criptomoeda na capitalização de mercado total.
logging.info("Iniciando Análise 3: Dominância de Mercado por Capitalização...")

# Cálculo da capitalização total do mercado
total_market_cap = df_latest_assats.agg(sum("market_cap_usd").alias("total_market_cap")).collect()[0]["total_market_cap"]

# Processamento apenas se há capitalização válida
if total_market_cap and total_market_cap > 0:
    df_market_dominance = df_latest_assats.filter(col("market_cap_usd").isNotNull()) \
                                        .withColumn("percent_market_cap", round((col("market_cap_usd") / lit(total_market_cap)) * 100, 4)) \
                                        .select(
                                            col("name"),
                                            col("symbol"),
                                            round(col("market_cap_usd"), 2).alias("market_cap_usd"),
                                            col("percent_market_cap"),
                                            col("data_referencia_dt").alias("data_referencia")
                                        ) \
                                        .orderBy(col("percent_market_cap").desc()) \
                                        .withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp")) # Adiciona a data/hora de execução
    # Remove valores nulos
    # Calcula percentual de participação
    # Nome da criptomoeda
    # Símbolo
    # Market cap individual
    # Percentual do mercado total
    # Data de referência
    # Ordena por dominância (maior primeiro)
    
    output_path_market_dominance = f"{output_curated_base_path}/market_dominance"
    write_analysis_data(df_market_dominance, output_path_market_dominance, "market_dominance")
    logging.info("Análise 3 concluída.")
else:
    logging.warning("Capitalização de mercado total é nula ou zero, pulando Análise 3.")
    
# ===== ANÁLISE 4: MÉTRICAS DE OFERTA E DEMANDA =====
# Calcula a capitalização de mercado por unidade de oferta e compara com a oferta máxima.
logging.info("Iniciando Análise 4: Métricas de Oferta e Demanda...")

df_supply_dynamics = df_latest_assats.filter(col("supply").isNotNull() & (col("supply") > 0) & col("market_cap_usd").isNotNull()) \
                                   .withColumn("market_cap_per_unit_supply", round(col("market_cap_usd") / col("supply"), 8)) \
                                   .select(
                                        col("name"),
                                        col("symbol"),
                                        round(col("supply"), 0).alias("supply"),
                                        round(col("max_supply"), 0).alias("max_supply"),
                                        col("market_cap_per_unit_supply"),
                                        when(col("max_supply").isNull(), lit("Não Definido")) \
                                            .otherwise(when(col("supply") >= col("max_supply"), lit("Próximo do Limite"))
                                                        .otherwise(lit("Disponível"))).alias("status_oferta_maxima"),
                                        col("data_referencia_dt").alias("data_referencia")
                                   ) \
                                   .orderBy(col("market_cap_per_unit_supply").desc()) \
                                   .withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp")) # Adiciona a data/hora de execução
# Filtra dados válidos
# Valor de mercado por unidade de supply
# Nome da criptomoeda
# Símbolo
# Oferta circulante atual
# Oferta máxima possível
# Market cap por unidade de supply
# Lógica condicional para status de escassez
# Data de referência
# Ordena por valor unitário (mais escasso primeiro)

output_path_supply_dynamics = f"{output_curated_base_path}/supply_dynamics"
write_analysis_data(df_supply_dynamics, output_path_supply_dynamics, "supply_dynamics")
logging.info("Análise 4 concluída.")

# RESULTADO FINAL: 4 análises estratégicas salvas em Delta Lake (Gold) e BigQuery
# PRÓXIMO PASSO: Consumo via Looker Studio para dashboards interativos