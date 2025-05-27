# AIRFLOW DAG PRINCIPAL - PIPELINE COMPLETA DE DADOS COINCAP
# Orquestra todo o fluxo: Pub/Sub → Cloud Run → Dataproc (Silver) → Dataproc (Gold) → BigQuery

# Importações necessárias para orquestração
from __future__ import annotations

import pendulum
import uuid
import logging

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from datetime import datetime
import time

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException
import time

# Configurações padrão aplicadas a todas as tasks da DAG
# Define os argumentos padrão para a DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": pendulum.duration(minutes=5),
}

# Função Python para trigger event-driven via Pub/Sub
# Função Python para publicar mensagem no Pub/Sub
def publish_to_pubsub_callable(project_id: str, topic_name: str):
    """
    Publica uma mensagem com um UUID único em um tópico do Pub/Sub.
    """
    hook = PubSubHook(gcp_conn_id='google_cloud_default') # Certifique-se que 'google_cloud_default' está configurado no Airflow

    message_id = str(uuid.uuid4()).encode('utf-8')

    logging.info(f"Publicando mensagem com UUID: {message_id.decode()} no tópico {topic_name} do projeto {project_id}")

    hook.publish(
        project_id=project_id,
        topic=topic_name,
        messages=[{'data': message_id}]
    )
    logging.info("Mensagem publicada com sucesso.")

# Função de validação para garantir que Cloud Run executou com sucesso
def validate_cloud_function_execution_callable():
    """
    Valida se existe pelo menos um arquivo coincap_data_*.json
    """
    
    logging.info("Validando execução do Cloud Run...")
    
    # Aguarda processamento
    time.sleep(30)
    
    # Verifica se existe arquivo com padrão coincap_data_*.json
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    try:
        # Lista arquivos no diretório
        blobs = gcs_hook.list("coincap", prefix="raw/assats_list/coincap_data_")
        
        # Filtra apenas arquivos .json
        json_files = [blob for blob in blobs if blob.endswith('.json')]
        
        if json_files:
            logging.info(f"Validação OK! Arquivo encontrado: {json_files[0]}")
            return True
        else:
            raise AirflowException("Cloud Run falhou - nenhum arquivo coincap_data_*.json encontrado.")
            
    except Exception as e:
        logging.error(f"Erro na validação: {str(e)}")
        raise AirflowException("Falha na validação do Cloud Run.")


# Definição da DAG principal com todas as configurações
with DAG(
    dag_id="assats-list-full",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Defina sua schedule aqui, por exemplo: "0 0 * * *" para rodar todo dia à meia-noite
    tags=["dataproc", "serverless", "pyspark", "pubsub", "cloudfunction"],
    default_args=default_args,
    description="DAG para executar um job PySpark no Dataproc Serverless, precedido por trigger Pub/Sub e validação de Cloud Function.",
) as dag:
    # Identificador único da DAG
    # Data de início (desenvolvimento)
    # Não executa backfill automático
    # Execução manual ou via trigger
    # Tags para organização
    # Configurações padrão aplicadas
    # Descrição da DAG
    
    # TASK 1: Inicialização do pipeline
    # Task de exemplo que pode ser uma task anterior na sua DAG
    start_task = BashOperator(
        task_id="start_processing",
        bash_command="echo 'Iniciando o processamento da DAG...'",
    )

    # TASK 2: Trigger Pub/Sub para ativar Cloud Run (extração Bronze)
    # Nova task para chamar um tópico Pub/Sub
    trigger_pubsub_message = PythonOperator(
        task_id="trigger_cloud_function_via_pubsub",
        python_callable=publish_to_pubsub_callable,
        op_kwargs={
            "project_id": "acoes-378306",
            "topic_name": "ps_coincap",
        },
    )

    # TASK 3: Validação de execução do Cloud Run
    # Nova task para validar a execução da Cloud Function
    validate_cloud_function_execution = PythonOperator(
        task_id="validate_cloud_function_get_data_coincap",
        python_callable=validate_cloud_function_execution_callable,
    )

    # TASK 4: Processamento Bronze → Silver (Dataproc Serverless)
    # Task existente para submeter o job PySpark no Dataproc Serverless
    processed_assats_list = BashOperator(
        task_id="processed_assats_list",
        bash_command=f"""
            gcloud dataproc batches submit \
                --project acoes-378306 \
                --region us-east1 \
                pyspark \
                --batch processed-assats-list-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \
                gs://cluster_spark/notebooks/jupyter/processed_assats_list.py \
                --version 1.1 \
                --subnet default \
                --service-account 1089008225676-compute@developer.gserviceaccount.com \
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.dataproc.driver.compute.tier=standard,spark.dataproc.executor.compute.tier=standard,spark.dataproc.appContext.enabled=true,internal.cohort=a5dc0113-290f-4612-af46-a0a99b778880,spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.dynamicAllocation.executorAllocationRatio=0.3,spark.app.name=projects/acoes-378306/locations/us-east1/batches/processed-assats-list,spark.dataproc.scaling.version=1 \
                --labels goog-dataproc-batch-id=processed-assats-list-v3,goog-dataproc-batch-uuid=a5dc0113-290f-4612-af46-a0a99b778880,goog-dataproc-location=us-east1,goog-dataproc-drz-resource-uuid=batch-a5dc0113-290f-4612-af46-a0a99b778880
        """,
    )
    # Projeto GCP
    # Região de execução
    # Tipo de job (PySpark)
    # Nome único com timestamp
    # Script PySpark Bronze→Silver
    # Versão Dataproc Serverless
    # Configuração de rede
    # Service Account com permissões
    # Configurações Spark + Delta Lake
    # Labels para tracking e billing

    # TASK 5: Processamento Silver → Gold (análises estratégicas)
    # Task Para Gerar Visão Analíticas (curated-analytics)
    curated_analytics = BashOperator(
        task_id="curated_analytics",
        bash_command=f"""
                gcloud dataproc batches submit \
                --project acoes-378306 \
                --region us-east1 pyspark \
                --batch curated-analytics-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \
                gs://cluster_spark/notebooks/jupyter/curated_analytics_full.py \
                --version 1.1 \
                --subnet default \
                --service-account 1089008225676-compute@developer.gserviceaccount.com \
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.dataproc.appContext.enabled=true
              """,
    )
    # Projeto GCP
    # Região + tipo job
    # Nome único com timestamp
    # Script PySpark Silver→Gold
    # Versão Dataproc Serverless
    # Configuração de rede
    # Service Account
    # Configurações Delta Lake + BigQuery



    # TASK 6: Finalização do pipeline
    # Task de exemplo que pode ser uma task subsequente na sua DAG
    end_task = BashOperator(
        task_id="end_processing",
        bash_command="echo 'Processamento da DAG finalizado!'",
    )

    # DEPENDÊNCIAS DA PIPELINE: Define ordem sequencial de execução
    # Definindo as dependências das tasks
    start_task >> trigger_pubsub_message >> validate_cloud_function_execution >> processed_assats_list >> curated_analytics >> end_task

# FLUXO COMPLETO:
# Start → Pub/Sub Trigger → Validação → Bronze→Silver → Silver→Gold → End
# Resultado: Dados prontos no BigQuery para consumo pelo Looker Studio