# AIRFLOW DAG PRINCIPAL - PIPELINE COMPLETA DE DADOS COINCAP
# Orquestra todo o fluxo: Pub/Sub → Cloud Run → Dataproc (Silver) → Dataproc (Gold) → BigQuery

# Importações necessárias
from __future__ import annotations    # Permite anotações de tipo modernas

import pendulum                      # Biblioteca avançada para datas (substitui datetime)
import uuid                         # Geração de identificadores únicos
import logging                      # Sistema de logs estruturados

from airflow.models.dag import DAG                           # Classe principal DAG
from airflow.operators.bash import BashOperator             # Operador para comandos bash
from airflow.operators.python import PythonOperator         # Operador para funções Python
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook  # Hook Pub/Sub do Google Cloud
from datetime import datetime       # Manipulação de timestamps
import time                        # Funções de tempo (sleep, etc.)

# CONFIGURAÇÕES PADRÃO DA DAG
# Aplicadas a todas as tasks para consistência
default_args = {
    "owner": "airflow",                    # Proprietário da DAG (auditoria)
    "depends_on_past": False,              # Tasks independentes de execuções anteriores
    "email_on_failure": False,             # Não envia email em falhas
    "email_on_retry": False,               # Não envia email em retries
    # "retries": 1,                        # Número de tentativas (desabilitado)
    # "retry_delay": pendulum.duration(minutes=5),  # Delay entre tentativas (desabilitado)
}

# FUNÇÃO PYTHON PARA TRIGGER PUB/SUB
def publish_to_pubsub_callable(project_id: str, topic_name: str):
    """
    Publica mensagem com UUID único no tópico Pub/Sub para trigger do Cloud Run
    
    Args:
        project_id: ID do projeto Google Cloud
        topic_name: Nome do tópico Pub/Sub
    
    Process:
        1. Gera UUID único para rastreamento
        2. Publica mensagem no tópico
        3. Triggera Cloud Run via event-driven architecture
    """
    # Inicializa hook Pub/Sub com conexão padrão do Airflow
    hook = PubSubHook(gcp_conn_id='google_cloud_default')  # Conexão deve estar configurada no Airflow

    # Gera identificador único para esta execução
    message_id = str(uuid.uuid4()).encode('utf-8')

    logging.info(f"Publicando mensagem com UUID: {message_id.decode()} no tópico {topic_name} do projeto {project_id}")

    # Publica mensagem no tópico (triggera Cloud Run automaticamente)
    hook.publish(
        project_id=project_id,           # Projeto GCP
        topic=topic_name,               # Tópico configurado
        messages=[{'data': message_id}] # Payload com UUID único
    )
    logging.info("Mensagem publicada com sucesso.")

# FUNÇÃO PYTHON PARA VALIDAÇÃO DE EXECUÇÃO
def validate_cloud_function_execution_callable():
    """
    Valida execução do Cloud Run antes de prosseguir para próxima etapa
    
    Process:
        1. Aguarda tempo para execução do Cloud Run
        2. Simula validação (em produção: verificar logs, arquivos, etc.)
        3. Garante que dados Bronze foram criados antes de processar Silver
    
    Note:
        Em produção real, implementar validações como:
        - Verificar logs no Cloud Logging
        - Confirmar arquivos no Cloud Storage
        - Consultar status em banco de dados
    """
    logging.info("Validando a execução da Cloud Function 'get-data-coincap'...")
    time.sleep(10)  # Aguarda execução (em produção: lógica real de validação)
    
    # EXEMPLO DE VALIDAÇÃO REAL (comentado):
    # from airflow.providers.google.cloud.hooks.gcs import GCSHook
    # gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    # if not gcs_hook.exists('coincap', 'raw/assats_list/'):
    #     raise AirflowException("Cloud Run não completou com sucesso - dados Bronze não encontrados")
    
    logging.info("Validação da Cloud Function concluída (simulada).")

# DEFINIÇÃO DA DAG PRINCIPAL
with DAG(
    dag_id="assats-list-full",                                      # ID único da DAG principal
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),            # Data de início (desenvolvimento)
    catchup=False,                                                  # Não executa backfill automático
    schedule=None,                                                  # Execução manual (não agendada)
    # schedule="0 0 * * *",                                         # Alternativa: execução diária à meia-noite
    tags=["dataproc", "serverless", "pyspark", "pubsub", "cloudfunction"],  # Tags para organização
    default_args=default_args,                                      # Configurações padrão
    description="DAG para executar um job PySpark no Dataproc Serverless, precedido por trigger Pub/Sub e validação de Cloud Function.",
) as dag:
    
    # ===== TASK 1: INICIALIZAÇÃO =====
    # Marca início do pipeline e registra logs de auditoria
    start_task = BashOperator(
        task_id="start_processing",                                 # ID único da task
        bash_command="echo 'Iniciando o processamento da DAG...'", # Comando simples de log
    )

    # ===== TASK 2: TRIGGER PUB/SUB → CLOUD RUN =====
    # Dispara extração de dados da API via Cloud Run event-driven
    trigger_pubsub_message = PythonOperator(
        task_id="trigger_cloud_function_via_pubsub",               # ID único da task
        python_callable=publish_to_pubsub_callable,               # Função Python a executar
        op_kwargs={                                                # Parâmetros da função
            "project_id": "acoes-378306",                          # Projeto GCP
            "topic_name": "ps_coincap",                            # Tópico Pub/Sub configurado
        },
    )

    # ===== TASK 3: VALIDAÇÃO CLOUD RUN =====
    # Garante que dados Bronze foram criados antes de prosseguir
    validate_cloud_function_execution = PythonOperator(
        task_id="validate_cloud_function_get_data_coincap",        # ID único da task
        python_callable=validate_cloud_function_execution_callable, # Função de validação
    )

    # ===== TASK 4: DATAPROC BRONZE → SILVER =====
    # Processa dados RAW (Bronze) e gera dados estruturados (Silver)
    processed_assats_list = BashOperator(
        task_id="processed_assats_list",                           # ID único da task
        bash_command=f"""
            gcloud dataproc batches submit \
                --project acoes-378306 \                          # Projeto GCP
                --region us-east1 \                               # Região de execução
                pyspark \                                         # Tipo de job (PySpark)
                --batch processed-assats-list-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \ # Nome único com timestamp
                gs://cluster_spark/notebooks/jupyter/processed_assats_list.py \ # Script PySpark Bronze→Silver
                --version 1.1 \                                   # Versão Dataproc Serverless
                --subnet default \                                # Configuração de rede
                --service-account 1089008225676-compute@developer.gserviceaccount.com \ # Service Account com permissões
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.dataproc.driver.compute.tier=standard,spark.dataproc.executor.compute.tier=standard,spark.dataproc.appContext.enabled=true,internal.cohort=a5dc0113-290f-4612-af46-a0a99b778880,spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.dynamicAllocation.executorAllocationRatio=0.3,spark.app.name=projects/acoes-378306/locations/us-east1/batches/processed-assats-list,spark.dataproc.scaling.version=1 \ # Configurações Spark + Delta Lake
                --labels goog-dataproc-batch-id=processed-assats-list-v3,goog-dataproc-batch-uuid=a5dc0113-290f-4612-af46-a0a99b778880,goog-dataproc-location=us-east1,goog-dataproc-drz-resource-uuid=batch-a5dc0113-290f-4612-af46-a0a99b778880 # Labels para tracking
        """,
    )

    # ===== TASK 5: DATAPROC SILVER → GOLD =====
    # Gera análises e insights (camada Curated/Gold)
    curated_analytics = BashOperator(
        task_id="curated_analytics",                               # ID único da task
        bash_command=f"""
                gcloud dataproc batches submit \
                --project acoes-378306 \                          # Projeto GCP
                --region us-east1 pyspark \                       # Região + tipo job
                --batch curated-analytics-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \ # Nome único com timestamp
                gs://cluster_spark/notebooks/jupyter/curated_analytics_full.py \ # Script PySpark Silver→Gold
                --version 1.1 \                                   # Versão Dataproc Serverless
                --subnet default \                                # Configuração de rede
                --service-account 1089008225676-compute@developer.gserviceaccount.com \ # Service Account
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.dataproc.appContext.enabled=true # Configurações Delta Lake + BigQuery
              """,
    )

    # ===== TASK 6: FINALIZAÇÃO =====
    # Marca conclusão do pipeline e registra logs finais
    end_task = BashOperator(
        task_id="end_processing",                                  # ID único da task
        bash_command="echo 'Processamento da DAG finalizado!'",   # Log de conclusão
    )

    # ===== DEFINIÇÃO DE DEPENDÊNCIAS (FLUXO DA PIPELINE) =====
    # Define ordem sequencial de execução das tasks
    # Fluxo: Start → Pub/Sub → Validação → Bronze→Silver → Silver→Gold → End
    start_task >> trigger_pubsub_message >> validate_cloud_function_execution >> processed_assats_list >> curated_analytics >> end_task

# ARQUITETURA COMPLETA DA PIPELINE:
# 1. start_task: Inicialização
# 2. trigger_pubsub_message: Pub/Sub → Cloud Run (extração API → Bronze)
# 3. validate_cloud_function_execution: Validação dados Bronze
# 4. processed_assats_list: Dataproc Bronze → Silver (dados estruturados)
# 5. curated_analytics: Dataproc Silver → Gold (análises) → BigQuery
# 6. end_task: Finalização
#
# RESULTADO FINAL: Dados prontos no BigQuery para consumo pelo Looker Studio