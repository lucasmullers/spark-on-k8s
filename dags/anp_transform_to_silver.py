from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import taskinstance
from airflow.utils.db import provide_session


DAG_ID = "TRANSFORM-ANP-DATA-SILVER"
DEFAULT_ARGS = {
    "owner": "Lucas Müller",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30)
}


@provide_session
def clear_tasks(tis, session=None, activate_dag_runs=False, dag=None) -> None:
    taskinstance.clear_task_instances(
        tis=tis,
        session=session,
        activate_dag_runs=activate_dag_runs,
        dag=dag,
    )


def clear_upstream_task(context):
    tasks_to_clear = context["params"].get("tasks_to_clear", [])
    all_tasks = context["dag_run"].get_task_instances()
    tasks_to_clear = [ti for ti in all_tasks if ti.task_id in tasks_to_clear]
    clear_tasks(tasks_to_clear, dag=context["dag"])


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    concurrency=1,
    schedule_interval="0 9 1,15 * *",
    catchup=True,
    dagrun_timeout=timedelta(minutes=45),
    tags=["ANP", "SILVER"],
    doc_md="DAG to Transforma ANP DATA from Landing Zone to Silver Layer",
) as dag:

    start = ExternalTaskSensor(
        task_id="start",
        external_dag_id="TRANSFORM-ANP-DATA-BRONZE",
        external_task_id="finish",
        timeout=7200,
        retries=500,
        execution_delta=timedelta(minutes=0),
        mode="reschedule",
        dag=dag
    )

    finish = EmptyOperator(task_id="finish", trigger_rule="all_success")

    job_cadastro_posto = SparkKubernetesOperator(
        task_id="create_silver_cadastro_posto",
        namespace="processing",
        application_file="spark-jobs/elt-anp-silver-cadastro-postos.yaml",
        kubernetes_conn_id="kubernetes_in_cluster",
        dag=dag,
    )

    monitor_cadastro_posto = SparkKubernetesSensor(
        task_id='monitor_create_silver_cadastro_posto',
        namespace='processing',
        application_name="{{ task_instance.xcom_pull(task_ids='create_silver_cadastro_posto')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_in_cluster",
        attach_log=True,
        on_retry_callback=clear_upstream_task,
        dag=dag,
    )

    job_informacao_venda = SparkKubernetesOperator(
        task_id="create_silver_informacao_venda",
        namespace="processing",
        application_file="spark-jobs/silver_informacao_venda.yaml",
        kubernetes_conn_id="kubernetes_in_cluster",
        dag=dag,
    )

    monitor_informacao_venda = SparkKubernetesSensor(
        task_id='monitor_create_silver_informacao_venda',
        namespace='processing',
        application_name="{{ task_instance.xcom_pull(task_ids='create_silver_informacao_venda')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_in_cluster",
        attach_log=True,
        on_retry_callback=clear_upstream_task,
        dag=dag,
    )

    _ = start >> job_cadastro_posto >> monitor_cadastro_posto >> finish
    _ = start >> job_informacao_venda >> monitor_informacao_venda >> finish
