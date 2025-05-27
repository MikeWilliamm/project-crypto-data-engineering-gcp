# CLOUD RUN - EXTRAÇÃO DE DADOS DA API COINCAP
# Função serverless que consome API CoinCap e salva dados RAW na camada Bronze (GCS)

# Importações necessárias
import base64                    # Decodificação de mensagens Pub/Sub (formato padrão Google Cloud)
import functions_framework       # Framework Google para Cloud Functions/Run
import requests                 # Cliente HTTP para consumir APIs REST
import json                     # Manipulação de dados JSON
from datetime import datetime   # Formatação de timestamps para nomenclatura de arquivos
from google.cloud import storage # Cliente oficial Google Cloud Storage

# Função principal - Event-driven (executada quando recebe mensagem do Pub/Sub)
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    """
    Função serverless que processa eventos do Pub/Sub para extrair dados da API CoinCap
    
    Args:
        cloud_event: Evento do Pub/Sub contendo dados da mensagem trigger
    
    Process:
        1. Decodifica mensagem Pub/Sub
        2. Chama API CoinCap para obter dados de criptomoedas
        3. Formata timestamp para nomenclatura única de arquivos
        4. Salva dados RAW no Google Cloud Storage (camada Bronze)
    """
    
    # STEP 1: Processamento da mensagem Pub/Sub
    # Decodifica dados da mensagem (formato Base64 padrão) para log de auditoria
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    
    # STEP 2: Configuração da API CoinCap
    # Endpoint v3 para listar todos os assets de criptomoedas disponíveis
    url = "https://rest.coincap.io/v3/assets"
    
    # Headers HTTP com autenticação Bearer Token e formato JSON
    headers = {
        "accept": "application/json",                                                           # Content-Type esperado
        "Authorization": "Bearer 31bdd92189b83afa6873720f1d05f4c7a7129dfcef60a224dcabf59474b670ee"  # API Key para autenticação
    }

    # STEP 3: Coleta de dados da API
    # Executa requisição GET para obter dados atuais do mercado crypto
    response = requests.get(url, headers=headers)
    response_json = response.json()  # Converte resposta HTTP em objeto Python
    
    # STEP 4: Geração de timestamp único para nomenclatura
    # Extrai timestamp Unix da própria resposta da API (garantindo consistência temporal)
    timestamp_unix_epoch = response_json.get("timestamp", 0)
    
    # Converte Unix timestamp (ms) para formato legível e remove caracteres especiais
    # Formato resultante: YYYYMMDD_HHMMSS (ex: 20240527_143022)
    data_formatada = str(datetime.utcfromtimestamp(timestamp_unix_epoch / 1000)).replace("-", "").replace(" ", "_").replace(":", '')[:15]
    
    # Define estrutura hierárquica e nome único do arquivo na camada Bronze
    file_name = f"raw/assats_list/coincap_data_{data_formatada}.json"

    # STEP 5: Configuração do Google Cloud Storage
    bucket_name = "coincap"                          # Bucket dedicado para dados CoinCap
    storage_client = storage.Client()                # Cliente oficial GCS
    bucket = storage_client.bucket(bucket_name)      # Referência ao bucket
    blob = bucket.blob(file_name)                    # Referência ao arquivo que será criado

    # STEP 6: Persistência dos dados RAW (camada Bronze)
    # Salva dados JSON formatados no GCS com metadados corretos
    blob.upload_from_string(
        data=json.dumps(response_json, indent=4, ensure_ascii=False),  # Pretty JSON com UTF-8
        content_type="application/json"                                # Metadado para facilitar processamento posterior
    )

    # STEP 7: Log de auditoria e confirmação
    # Registra sucesso da operação no Cloud Logging para monitoramento
    print(f"Arquivo {file_name} salvo no bucket {bucket_name}.")