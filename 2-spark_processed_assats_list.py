# AIRFLOW DAG - ORQUESTRAÇÃO DO PIPELINE DE DADOS COINCAP
# DAG responsável por executar job PySpark no Dataproc Serverless para transformação Bronze → Silver

# Importações necessárias
from __future__ import annotations    # Permite anotações de tipo modernas (Python 3.7+)
import pendulum                      # Biblioteca para manipulação de datas/horários (melhor que datetime)

from airflow.models.dag import DAG           # Classe principal para definição de DAGs
from airflow.operators.bash import BashOperator  # Operador para executar comandos bash/shell

# Configurações padrão aplicadas a todas as tasks da DAG
default_args = {
    "owner": "airflow",                              # Proprietário da DAG (para auditoria)
    "depends_on_past": False,                        # Tasks não dependem do sucesso da execução anterior
    "email_on_failure": False,                       # Não envia email em caso de falha
    "email_on_retry": False,                         # Não envia email em caso de retry
    "retries": 1,                                    # Número máximo de tentativas em caso de falha
    "retry_delay": pendulum.duration(minutes=5),     # Intervalo entre tentativas (5 minutos)
}

# Definição da DAG principal
with DAG(
    dag_id="dataproc_serverless_batch_dag",                     # Identificador único da DAG
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),        # Data de início (histórica para desenvolvimento)
    catchup=False,                                              # Não executa backfill automático
    schedule=None,                                              # Execução manual ou via trigger (não agendada)
    # schedule="0 0 * * *",                                     # Alternativa: executar diariamente à meia-noite
    tags=["dataproc", "serverless", "pyspark"],                # Tags para organização/filtros na UI
    default_args=default_args,                                  # Aplica configurações padrão
    description="DAG para executar um job PySpark no Dataproc Serverless.", # Descrição da DAG
) as dag:
    
    # TASK 1: Inicialização do pipeline
    # Task simples para marcar início do processamento e logs de auditoria
    start_task = BashOperator(
        task_id="start_processing",                             # ID único da task
        bash_command="echo 'Iniciando o processamento da DAG...'",  # Comando bash simples
    )

    # TASK 2: Submissão do job PySpark no Dataproc Serverless
    # Esta é a task principal que executa a transformação Bronze → Silver
    submit_dataproc_serverless_job = BashOperator(
        task_id="submit_dataproc_serverless_pyspark_job",       # ID único da task principal
        bash_command="""
            gcloud dataproc batches submit \
                --project acoes-378306 \                        # Projeto Google Cloud
                --region us-east1 \                             # Região para execução (próxima aos dados)
                pyspark \                                       # Tipo de job (PySpark)
                --batch processed-assats-list-v4 \              # Nome único do batch job
                gs://cluster_spark/notebooks/jupyter/processed_assats_list.py \ # Localização do script PySpark
                --version 1.1 \                                 # Versão do Dataproc Serverless
                --subnet default \                              # Subnet de rede para o cluster
                --service-account 1089008225676-compute@developer.gserviceaccount.com \ # Service Account com permissões necessárias
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.dataproc.driver.compute.tier=standard,spark.dataproc.executor.compute.tier=standard,spark.dataproc.appContext.enabled=true,internal.cohort=a5dc0113-290f-4612-af46-a0a99b778880,spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.dynamicAllocation.executorAllocationRatio=0.3,spark.app.name=projects/acoes-378306/locations/us-east1/batches/processed-assats-list,spark.dataproc.scaling.version=1 \ # Configurações do Spark:
                # - Delta Lake dependencies para ACID transactions
                # - Configurações de recursos (2 executors, 4 cores cada)
                # - Extensões SQL para Delta Lake
                # - Configurações de scaling automático
                --labels goog-dataproc-batch-id=processed-assats-list-v3,goog-dataproc-batch-uuid=a5dc0113-290f-4612-af46-a0a99b778880,goog-dataproc-location=us-east1,goog-dataproc-drz-resource-uuid=batch-a5dc0113-290f-4612-af46-a0a99b778880 # Labels para tracking e billing
        """,
    )

    # TASK 3: Finalização do pipeline
    # Task para marcar conclusão do processamento e logs de auditoria
    end_task = BashOperator(
        task_id="end_processing",                               # ID único da task final
        bash_command="echo 'Processamento da DAG finalizado!'", # Comando bash simples
    )

    # DEPENDÊNCIAS DAS TASKS - Define ordem de execução
    # Fluxo linear: início → processamento Spark → fim
    # Operador >> define precedência (task_anterior >> task_posterior)
    start_task >> submit_dataproc_serverless_job >> end_task

# ARQUITETURA DO PIPELINE:
# 1. start_task: Log de início
# 2. submit_dataproc_serverless_job: Executa transformação Bronze → Silver via Spark
# 3. end_task: Log de conclusão
#
# RECURSOS CONFIGURADOS:
# - Dataproc Serverless: Sem gerenciamento de cluster
# - Delta Lake: Versionamento e ACID transactions
# - 2 executors com 4 cores cada: Balanceamento custo/performance
# - Service Account: Permissões para acessar GCS e BigQuery