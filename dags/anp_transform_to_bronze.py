from functions.transform_to_bronze import create_cadastro_posto_table, create_informacao_venda_table

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

DAG_ID = "TRANSFORM-ANP-DATA-BRONZE"
DEFAULT_ARGS = {
    "owner": "Lucas Müller",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    concurrency=1,
    schedule_interval="0 9 1,15 * *",
    catchup=True,
    dagrun_timeout=timedelta(minutes=45),
    tags=["ANP", "BRONZE"],
    doc_md="DAG to Transforma ANP DATA from Landing Zone to Bronze Layer",
) as dag:

    start = ExternalTaskSensor(
        task_id="start",
        external_dag_id="EXTRACT-ANP-DATA-LANDING-ZONE",
        external_task_id="finish",
        timeout=7200,
        retries=500,
        execution_delta=timedelta(minutes=0),
        mode="reschedule",
        dag=dag
    )

    finish = EmptyOperator(task_id="finish", trigger_rule="all_success")

    anp_bronze_layer = SparkKubernetesOperator(
        task_id='copy_anp_data_to_bronze_layer',
        namespace='processing',
        application_file='spark-jobs/anp_bronze_layer.yaml',
        kubernetes_conn_id='kubernetes_in_cluster',
    )

    _ = start >> anp_bronze_layer >> finish
