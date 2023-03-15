from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models import taskinstance
from airflow.utils.db import provide_session


DAG_ID = "TRANSFORM-ANP-DATA-BRONZE"
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

    # run_job = SparkKubernetesOperator(
    #     task_id="execute_copy_anp_data_to_bronze_layer",
    #     namespace="processing",
    #     application_file="spark-jobs/elt-anp-bronze.yaml",
    #     kubernetes_conn_id="kubernetes_in_cluster",
    #     dag=dag,
    # )

    # spark_submit = SparkSubmitOperator(
    #     task_id="execute_copy_anp_data_to_bronze_layer",
    #     application="s3a://spark-k8s-scripts/convert_tables_to_delta.py",
    #     executor_memory="6144m",
    #     num_executors="2"
    #     name="elt-anp-bronze-{{ ds }}-{{ task_instance.try_number }}",
    #     conf={
    #         "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    #         "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #         "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #         "spark.hadoop.fs.s3a.path.style.access": "True",
    #         "spark.hadoop.fs.s3a.fast.upload": "True",
    #         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    #         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #         "spark.driver.cores": "1",
    #         "spark.driver.memoryOverhead": "1024m",
    #         "spark.driver.memory": "4000m",
    #         "spark.spark.executor.instances": "2",
    #         "spark.executor.cores": "4",
    #         "spark.executor.memoryOverhead": "512m",
    #         "spark.executor.memory": "2048m",
    #         "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-operator-spark",
    #         "spark.kubernetes.namespace": "processing"
    #     },
    #     total_executor_cores=4,
    #     verbose=True,
    #     dag=dag,
    # )

    submit_job = SparkSubmitOperator(
        application="local:///opt/spark/examples/jars/spark-examples_2.12-3.3.2.jar", task_id="submit_job"

    )

    # monitor = SparkKubernetesSensor(
    #     task_id='monitor_copy_anp_data_to_bronze_layer',
    #     namespace='processing',
    #     application_name="{{ task_instance.xcom_pull(task_ids='execute_copy_anp_data_to_bronze_layer')['metadata']['name'] }}",
    #     kubernetes_conn_id="kubernetes_in_cluster",
    #     attach_log=True,
    #     on_retry_callback=clear_upstream_task,
    #     dag=dag,
    # )

    _ = start >> submit_job >> finish
