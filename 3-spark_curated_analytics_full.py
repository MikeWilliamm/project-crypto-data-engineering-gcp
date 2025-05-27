# PYSPARK - CAMADA CURATED (GOLD) - ANÁLISES DE DADOS DE CRIPTOMOEDAS
# Job Spark que processa dados da camada Silver e gera análises na camada Gold/Curated

# Importações necessárias
import logging                          # Sistema de logs estruturados
import os                              # Variáveis de ambiente do sistema
from datetime import datetime          # Manipulação de timestamps
from pyspark.sql import SparkSession   # Sessão principal do Spark
from pyspark.sql.functions import lit, sha2, concat_ws, col, expr, row_number, trim, upper, when, explode, from_unixtime, sum, avg, round, percent_rank  # Funções SQL do Spark
from google.cloud import storage       # Cliente Google Cloud Storage
import argparse                        # Parser de argumentos de linha de comando
import sys                            # Funções do sistema (exit, etc.)
from delta import *                   # Delta Lake para ACID transactions
from google.cloud import bigquery     # Cliente BigQuery
from pyspark.sql.window import Window  # Window functions para análises temporais

# CONFIGURAÇÃO INICIAL
# Sistema de logging estruturado para monitoramento
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# INICIALIZAÇÃO DA SESSÃO SPARK
# Configura Spark com Delta Lake e conectores BigQuery
logging.info("Inicializando SparkSession...")
spark = SparkSession.builder \
    .appName("CryptoAnalyticsCuratedLayer") \                                                           # Nome da aplicação
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \                       # Extensões SQL Delta Lake
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \   # Catálogo Delta
    .config("spark.jars.packages", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.1.jar") \  # Conector BigQuery
    .getOrCreate()
logging.info("SparkSession inicializada com sucesso.")

# VARIÁVEIS DE CONTROLE TEMPORAL
# Timestamps para auditoria e nomenclatura de arquivos
date_time_exec = datetime.now()                                                # Timestamp atual
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S")              # Formato: 20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S")      # Formato: 2024-05-01 13:05:11

# CONFIGURAÇÃO DE CAMINHOS
# Estrutura de diretórios na arquitetura medalion
input_path = f"gs://coincap/processed/assats_list"                             # Camada Silver (dados processados)
output_curated_base_path = f"gs://coincap/curated/crypto_analytics"            # Camada Gold (análises)

# CONFIGURAÇÃO BIGQUERY
# Projeto e dataset de destino para análises
bigquery_project = "acoes-378306"
bigquery_dataset = "coincap"

# CARREGAMENTO DOS DADOS DA CAMADA SILVER
# Lê Delta Table da camada Silver com dados processados das criptomoedas
logging.info(f"Lendo dados da tabela Delta em: {input_path}")
try:
    df_assats_list = DeltaTable.forPath(spark, input_path).toDF()               # Carrega Delta Table como DataFrame
    logging.info(f"DataFrame base 'df_assats_list' carregado com {df_assats_list.count()} registros.")
except Exception as e:
    logging.error(f"Erro ao carregar df_assats_list: {e}")
    sys.exit(1)                                                                 # Encerra processo em caso de erro

# PREPARAÇÃO DOS DADOS PARA ANÁLISE
# Adiciona colunas de metadados e converte tipos para análises temporais
df_assats_list = df_assats_list.withColumn("data_referencia_dt", col("data_referencia").cast("timestamp"))  # Converte para timestamp

# WINDOW FUNCTION PARA DADOS MAIS RECENTES
# Define janela particionada por ID para obter registro mais atual de cada crypto
window_spec = Window.partitionBy("id").orderBy(col("data_referencia_dt").desc())

# Filtra apenas o registro mais recente de cada criptomoeda (snapshot atual)
df_latest_assats = df_assats_list \
    .withColumn("rn", row_number().over(window_spec)) \    # Adiciona número da linha (1 = mais recente)
    .filter(col("rn") == 1) \                             # Filtra apenas a linha 1 (mais recente)
    .drop("rn")                                           # Remove coluna auxiliar

# FUNÇÃO AUXILIAR PARA PERSISTÊNCIA
def write_analysis_data(df, delta_output_path, bq_table_name):
    """
    Função para escrever dados simultaneamente em Delta Lake e BigQuery
    
    Args:
        df: DataFrame Spark com dados da análise
        delta_output_path: Caminho no GCS para salvar Delta Table
        bq_table_name: Nome da tabela no BigQuery
    
    Process:
        1. Salva em Delta Lake (append para histórico)
        2. Salva em BigQuery (overwrite para análises atuais)
    """
    # PERSISTÊNCIA EM DELTA LAKE (CAMADA GOLD)
    # Modo append para manter histórico de todas as análises
    logging.info(f"Escrevendo '{bq_table_name}' para Delta em: {delta_output_path} (modo append)")
    df.write \
        .format("delta") \                                 # Formato Delta Lake
        .mode("append") \                                  # Preserva histórico
        .option("mergeSchema", "true") \                   # Permite evolução de schema
        .save(delta_output_path)
    logging.info(f"Dados do Delta para '{bq_table_name}' salvos.")

    # PERSISTÊNCIA EM BIGQUERY (PARA CONSULTAS)
    # Modo overwrite para manter apenas dados mais atuais no BigQuery
    logging.info(f"Escrevendo '{bq_table_name}' para BigQuery em: {bigquery_project}.{bigquery_dataset}.{bq_table_name} (modo overwrite)")
    df.write.format("bigquery") \
        .option("temporaryGcsBucket", f"logs-apps/{bq_table_name}") \  # Bucket temporário para staging
        .option("project", 'acoes-378306') \               # Projeto GCP
        .option("dataset", 'coincap') \                    # Dataset BigQuery
        .option("table", bq_table_name) \                  # Nome da tabela
        .mode("overwrite") \                               # Substitui dados anteriores
        .save()
    logging.info(f"Dados do BigQuery para '{bq_table_name}' salvos.")

# ===== ANÁLISE 1: VISÃO GERAL DIÁRIA (DAILY SNAPSHOT) =====
# Snapshot atual de todas as criptomoedas com métricas principais
logging.info("Iniciando Análise 1: Visão Geral Diária (Daily Snapshot)...")

df_daily_overview = df_latest_assats.select(
    col("id"),                                             # Identificador único
    col("name"),                                           # Nome da criptomoeda
    col("symbol"),                                         # Símbolo (BTC, ETH, etc.)
    col("rank"),                                           # Ranking por market cap
    round(col("price_usd"), 8).alias("price_usd"),        # Preço em USD (8 casas decimais)
    round(col("market_cap_usd"), 2).alias("market_cap_usd"),  # Market cap em USD
    round(col("volume_usd_24hr"), 2).alias("volume_usd_24hr"),  # Volume 24h
    round(col("change_percent_24hr"), 4).alias("change_percent_24hr"),  # Variação 24h
    round(col("vwap_24hr"), 8).alias("vwap_24hr"),        # Preço médio ponderado por volume
    round(col("supply"), 0).alias("supply"),              # Oferta circulante
    round(col("max_supply"), 0).alias("max_supply"),      # Oferta máxima
    col("explorer"),                                       # Link do explorer blockchain
    col("data_referencia_dt").alias("data_referencia")    # Data/hora de referência
).orderBy(col("rank").asc()) \                            # Ordena por ranking
.withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp"))  # Timestamp do processamento

output_path_daily_overview = f"{output_curated_base_path}/daily_overview"
write_analysis_data(df_daily_overview, output_path_daily_overview, "daily_overview")
logging.info("Análise 1 concluída.")

# ===== ANÁLISE 2: TOP GANHADORES E PERDEDORES (24HR) =====
# Identifica maiores altas e baixas nas últimas 24 horas
logging.info("Iniciando Análise 2: Top Ganhadores e Perdedores (24hr)...")

# TOP 10 GANHADORES (maiores altas)
df_gainers = df_latest_assats.filter(col("change_percent_24hr").isNotNull()) \  # Remove valores nulos
                            .orderBy(col("change_percent_24hr").desc()) \       # Ordena por maior variação
                            .limit(10) \                                        # Top 10
                            .withColumn("tipo_movimento", lit("Ganhador"))      # Flag identificadora

# TOP 10 PERDEDORES (maiores baixas)
df_losers = df_assats_list.filter(col("change_percent_24hr").isNotNull()) \     # Remove valores nulos
                           .orderBy(col("change_percent_24hr").asc()) \         # Ordena por menor variação
                           .limit(10) \                                         # Top 10
                           .withColumn("tipo_movimento", lit("Perdedor"))       # Flag identificadora

# UNIÃO DOS DATASETS
df_top_gainers_losers = df_gainers.unionAll(df_losers).select(
    col("name"),                                           # Nome da crypto
    col("symbol"),                                         # Símbolo
    round(col("change_percent_24hr"), 4).alias("change_percent_24hr"),  # Variação percentual
    round(col("price_usd"), 8).alias("price_usd"),        # Preço atual
    col("tipo_movimento"),                                 # Ganhador/Perdedor
    col("data_referencia_dt").alias("data_referencia")    # Data referência
) \
.withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp"))  # Timestamp processamento

output_path_top_gainers_losers = f"{output_curated_base_path}/top_gainers_losers"
write_analysis_data(df_top_gainers_losers, output_path_top_gainers_losers, "top_gainers_losers")
logging.info("Análise 2 concluída.")

# ===== ANÁLISE 3: DOMINÂNCIA DE MERCADO POR CAPITALIZAÇÃO =====
# Calcula participação percentual de cada crypto no mercado total
logging.info("Iniciando Análise 3: Dominância de Mercado por Capitalização...")

# Calcula capitalização total do mercado
total_market_cap = df_latest_assats.agg(sum("market_cap_usd").alias("total_market_cap")).collect()[0]["total_market_cap"]

if total_market_cap and total_market_cap > 0:
    df_market_dominance = df_latest_assats.filter(col("market_cap_usd").isNotNull()) \  # Remove valores nulos
                                        .withColumn("percent_market_cap", round((col("market_cap_usd") / lit(total_market_cap)) * 100, 4)) \  # Calcula percentual
                                        .select(
                                            col("name"),                               # Nome crypto
                                            col("symbol"),                             # Símbolo
                                            round(col("market_cap_usd"), 2).alias("market_cap_usd"),  # Market cap
                                            col("percent_market_cap"),                 # Percentual do mercado
                                            col("data_referencia_dt").alias("data_referencia")  # Data referência
                                        ) \
                                        .orderBy(col("percent_market_cap").desc()) \  # Ordena por dominância
                                        .withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp"))
    
    output_path_market_dominance = f"{output_curated_base_path}/market_dominance"
    write_analysis_data(df_market_dominance, output_path_market_dominance, "market_dominance")
    logging.info("Análise 3 concluída.")
else:
    logging.warning("Capitalização de mercado total é nula ou zero, pulando Análise 3.")

# ===== ANÁLISE 4: MÉTRICAS DE OFERTA E DEMANDA =====
# Analisa dinâmica de supply e scarcity das criptomoedas
logging.info("Iniciando Análise 4: Métricas de Oferta e Demanda...")

df_supply_dynamics = df_latest_assats.filter(col("supply").isNotNull() & (col("supply") > 0) & col("market_cap_usd").isNotNull()) \  # Filtra dados válidos
                                   .withColumn("market_cap_per_unit_supply", round(col("market_cap_usd") / col("supply"), 8)) \  # Market cap por unidade
                                   .select(
                                        col("name"),                               # Nome crypto
                                        col("symbol"),                             # Símbolo
                                        round(col("supply"), 0).alias("supply"),  # Oferta circulante
                                        round(col("max_supply"), 0).alias("max_supply"),  # Oferta máxima
                                        col("market_cap_per_unit_supply"),         # Valor por unidade supply
                                        when(col("max_supply").isNull(), lit("Não Definido")) \     # Lógica condicional para status
                                            .otherwise(when(col("supply") >= col("max_supply"), lit("Próximo do Limite"))
                                                        .otherwise(lit("Disponível"))).alias("status_oferta_maxima"),
                                        col("data_referencia_dt").alias("data_referencia")  # Data referência
                                   ) \
                                   .orderBy(col("market_cap_per_unit_supply").desc()) \  # Ordena por valor unitário
                                   .withColumn("data_processamento_analise", lit(data_hora_inicio_format_db).cast("timestamp"))

output_path_supply_dynamics = f"{output_curated_base_path}/supply_dynamics"
write_analysis_data(df_supply_dynamics, output_path_supply_dynamics, "supply_dynamics")
logging.info("Análise 4 concluída.")

# PIPELINE COMPLETO:
# Silver (dados processados) → Análises Gold → Delta Lake + BigQuery → Looker Studio