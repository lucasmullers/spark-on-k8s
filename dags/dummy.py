from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

DAG_ID = "Dummy-DAG"
DEFAULT_ARGS = {
    "owner": "Datarisk.io",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    concurrency=3,
    schedule_interval="0 9 * * *",
    catchup=True,
    dagrun_timeout=timedelta(hours=1),
    tags=["Dummy", "ML-Dummy"],
    doc_md="DAG dummy de teste do deploy",
) as dag:

    start = DummyOperator(task_id="start")

    finish = DummyOperator(task_id="finish", trigger_rule="all_done")

    _ = start >> finish
