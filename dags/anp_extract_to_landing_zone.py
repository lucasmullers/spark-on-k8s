from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from functions.download_table_anp import download_and_load_to_lake


def scrappy_available_tables():
    import requests
    from bs4 import BeautifulSoup

    base_url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-" + \
               "combustiveis"
    response = requests.get(base_url)
    # Cria o parser do html
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontra todas as urls da pagina
    all_links = soup.find_all('a')

    # Filtra link que possuam shpc/dsas/ca/ pois são os links de interesse nesse projeto
    url_of_tables_to_download = [link.get('href') for link in all_links if 'shpc/dsas/ca/' in link.get('href', [])]
    return url_of_tables_to_download


DAG_ID = "EXTRACT-ANP-DATA-BRONZE"
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
    concurrency=5,
    schedule_interval="0 9 1,15 * *",
    catchup=True,
    dagrun_timeout=timedelta(minutes=45),
    tags=["ANP", "LANDING-ZOE"],
    doc_md="DAG to extract ANP DATA to Landing Zone",
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish", trigger_rule="all_success")

    url_tables_to_download = scrappy_available_tables()

    for url in url_tables_to_download:
        download_table = PythonOperator(
            task_id=f"download_file_{url.split('/')[-1].split('.')[0]}",
            python_callable=download_and_load_to_lake,
            op_kwargs={
                "url": url,
                "minio_connection_id": "minio"
            },
            trigger_rule="all_success"
        )

        start >> download_table >> finish
