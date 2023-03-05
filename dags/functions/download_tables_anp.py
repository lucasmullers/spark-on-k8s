import requests
import zipfile
import io
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_files_on_lakehouse(s3_hook: S3Hook, bucket_name: str = "etl-lakehouse"):
    files_on_lakehouse = s3_hook.list_keys(bucket_name=bucket_name, prefix="LANDING_ZONE/anp/")
    files_on_lakehouse = [file.split("/")[-1] for file in files_on_lakehouse]
    print("Files on lake response: ", files_on_lakehouse)

    return files_on_lakehouse


def download_and_load_to_lake(url: str = "", conn_id: str = "aws"):
    s3_hook = S3Hook(conn_id)

    files_already_on_lake = get_files_on_lakehouse(s3_hook=s3_hook)

    file = url.split("/")[-1]

    # Se for csv e já estiver baixado eu retorn
    if ".csv" in url and file in files_already_on_lake:
        print("File already on lake!")
        return

    print(f"Downloading file: {file}")
    r = requests.get(url, allow_redirects=True)

    # Se for CSV não está no lake, então eu baixo e subo no lake
    if ".csv" in url:
        with open(file, "wb") as f:
            f.write(r.content)
        s3_hook.load_file(file, f"LANDING_ZONE/anp/{file}", "etl-lakehouse")
        os.remove(file)
    # Se for zip eu baixo e unzipo para verificar se CSV está no lake
    elif ".zip" in url:
        # Cria arquivo zip
        zip_file = zipfile.ZipFile(io.BytesIO(r.content))
        # Extrai localmente
        zip_file.extractall("./file.zip")
        # Pega nome do primeiro arquivo no zip
        unziped_file_name = zip_file.namelist()[0]

        # Se arquivo não estiver no lake então sobre
        if unziped_file_name not in files_already_on_lake:
            file = f"./file.zip/{unziped_file_name}"
            s3_hook.load_file(file, f"LANDING_ZONE/anp/{file.split('/')[-1]}", "etl-lakehouse")
