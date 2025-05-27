# PYSPARK JOB - BRONZE TO SILVER LAYER
# Processa dados RAW (Bronze) e transforma em dados estruturados (Silver) usando Delta Lake

# Importações necessárias para processamento de dados
import logging                          # Sistema de logs estruturados
import os                              # Manipulação de caminhos e arquivos
from datetime import datetime          # Manipulação de timestamps
from pyspark.sql import SparkSession   # Sessão principal do Spark
from pyspark.sql.functions import lit, sha2, concat_ws, col, expr, row_number, trim, upper, when, explode, from_unixtime  # Funções SQL do Spark
from google.cloud import storage       # Cliente Google Cloud Storage
import argparse                        # Parser de argumentos
import sys                            # Funções do sistema
from delta import *                   # Delta Lake para ACID transactions
from google.cloud import bigquery     # Cliente BigQuery
from pyspark.sql.window import Window  # Window functions para análises

# Configuração do sistema de logging para monitoramento
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# Inicialização da sessão Spark com configurações Delta Lake e BigQuery
spark = SparkSession.builder \
    .appName("ValidateDeltaTables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.1.jar") \
    .getOrCreate()
# Nome da aplicação Spark
# Extensões SQL para Delta Lake
# Catálogo Delta
# Conector BigQuery

# Definição de variáveis temporais para controle de execução
date_time_exec = datetime.now()                                                # Timestamp da execução atual
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S")              # Formato para nomes de arquivo: 20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S")      # Formato para banco: 2024-05-01 13:05:11

# Definição de caminhos para leitura (Bronze) e escrita (Silver)
input_path = f"gs://coincap/raw/assats_list/coincap_data_*.json"               # Caminho dos arquivos JSON RAW (wildcard para múltiplos arquivos)
output_path = f"gs://coincap/processed/assats_list"                            # Caminho de saída para Delta Table (Silver)

# Leitura dos dados RAW da camada Bronze
raw_df = spark.read.option("multiline", "true").json(input_path)              # Lê JSONs com suporte a múltiplas linhas

# Explosão da estrutura JSON: transforma array "data" em linhas individuais
coins_df = raw_df.select(explode("data").alias("coin"), raw_df["timestamp"])  # Cada elemento do array vira uma linha

# Transformação e estruturação dos dados: extrai campos aninhados e aplica tipagem
final_df = coins_df.select(
    col("coin.id").alias("id"),                                                # ID único da criptomoeda
    col("coin.rank").cast("int").alias("rank"),                               # Ranking por market cap (convertido para inteiro)
    col("coin.symbol").alias("symbol"),                                        # Símbolo da crypto (BTC, ETH, etc.)
    col("coin.name").alias("name"),                                           # Nome completo da criptomoeda
    col("coin.supply").cast("double").alias("supply"),                        # Oferta circulante (convertido para double)
    col("coin.maxSupply").cast("double").alias("max_supply"),                 # Oferta máxima (convertido para double)
    col("coin.marketCapUsd").cast("double").alias("market_cap_usd"),          # Capitalização de mercado em USD
    col("coin.volumeUsd24Hr").cast("double").alias("volume_usd_24hr"),        # Volume de negociação 24h em USD
    col("coin.priceUsd").cast("double").alias("price_usd"),                   # Preço atual em USD
    col("coin.changePercent24Hr").cast("double").alias("change_percent_24hr"), # Variação percentual 24h
    col("coin.vwap24Hr").cast("double").alias("vwap_24hr"),                   # Preço médio ponderado por volume 24h
    col("coin.explorer").alias("explorer"),                                   # URL do explorer da blockchain
    from_unixtime(col("timestamp") / 1000).alias("data_referencia"),          # Converte timestamp Unix para datetime
    lit(data_hora_inicio_format_db).alias("data_processamento")               # Adiciona timestamp do processamento
)

# Persistência na camada Silver usando Delta Lake
final_df.write.format("delta").mode("append").save(output_path)               # Salva como Delta Table com modo append (preserva histórico)

# Marcação dos arquivos processados: adiciona prefixo "read_" para controle
"""Renomeia os arquivos JSON processados adicionando o prefixo 'read_' no nome."""
bucket_name = "coincap"                                                        # Nome do bucket no Cloud Storage
prefix = "raw/assats_list/"                                                    # Prefixo do diretório dos arquivos RAW
client = storage.Client()                                                      # Cliente do Google Cloud Storage
bucket = client.bucket(bucket_name)                                           # Referência ao bucket
blobs = list(bucket.list_blobs(prefix=prefix))                               # Lista todos os arquivos no diretório

# Loop para renomear arquivos processados
for blob in blobs:
    filename = os.path.basename(blob.name)                                     # Extrai apenas o nome do arquivo
    dirname = os.path.dirname(blob.name)                                       # Extrai o diretório
    
    # Ignora arquivos já processados ou que não são JSON
    if filename.startswith("read_") or not filename.endswith(".json"):
        continue
    
    # Cria novo nome com prefixo "read_" para marcar como processado
    new_name = os.path.join(dirname, f"read_{filename}")
    new_blob = bucket.rename_blob(blob, new_name)                             # Executa a renomeação
    logging.info(f"Arquivo renomeado: {blob.name} -> {new_blob.name}")        # Log da operação

# RESULTADO: Dados estruturados e tipados salvos na camada Silver (Delta Lake)
# PRÓXIMO PASSO: Processamento Silver → Gold para análises estratégicas