import requests
import json
from datetime import datetime

# URL e headers da API
url = "https://rest.coincap.io/v3/assets"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer 31bdd92189b83afa6873720f1d05f4c7a7129dfcef60a224dcabf59474b670ee"
}

# Requisição
response = requests.get(url, headers=headers)
response_json = response.json()

# Timestamp formatado
timestamp_unix_epoch = response_json.get("timestamp", 0)
data_formatada = str(datetime.utcfromtimestamp(timestamp_unix_epoch / 1000)).replace("-", "").replace(" ", "_").replace(":", '')[:15]

# Caminho e nome do arquivo
file_name = f"C:\\Users\\mike-\\Desktop\\cadastra\\coincap_{data_formatada}.json"

# Salvando o JSON completo localmente
with open(file_name, "w", encoding="utf-8") as f:
    json.dump(response_json, f, indent=4, ensure_ascii=False)

print(f"Arquivo salvo como: {file_name}")


#CLOUD RUN:
# import base64
# import functions_framework
# import requests
# import json
# from datetime import datetime
# from google.cloud import storage

# # Triggered from a message on a Cloud Pub/Sub topic.
# @functions_framework.cloud_event
# def hello_pubsub(cloud_event):
#     # Print out the data from Pub/Sub, to prove that it worked
#     print(base64.b64decode(cloud_event.data["message"]["data"]))
    
#     # URL e headers da API
#     url = "https://rest.coincap.io/v3/assets"
#     headers = {
#     "accept": "application/json",
#     "Authorization": "Bearer 31bdd92189b83afa6873720f1d05f4c7a7129dfcef60a224dcabf59474b670ee"
#     }

#     # Requisição
#     response = requests.get(url, headers=headers)
#     response_json = response.json()
    
#     # Timestamp formatado
#     timestamp_unix_epoch = response_json.get("timestamp", 0)
#     data_formatada = str(datetime.utcfromtimestamp(timestamp_unix_epoch / 1000)).replace("-", "").replace(" ", "_").replace(":", '')[:15]
#     file_name = f"raw/assats_list/coincap_data_{data_formatada}.json"

#     # Inicializa o cliente do GCS
#     bucket_name = "coincap"
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(file_name)

#     # Salva o conteúdo JSON no bucket como string
#     blob.upload_from_string(
#         data=json.dumps(response_json, indent=4, ensure_ascii=False),
#         content_type="application/json"
#     )

#     print(f"Arquivo {file_name} salvo no bucket {bucket_name}.")