from pyspark.sql import SparkSession


def create_spark_session(connection_id: str = "aws"):
    spark = (
        SparkSession
        .builder
        .config("com.amazonaws.services.s3.enableV4", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .config("fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    return spark


def delete_files_on_s3(bucket_name: str = "etl-data-lakehouse", path: str = "BRONZE/anp/", conn_id: str = "aws"):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(conn_id)
    s3_hook.delete_objects(bucket=bucket_name, keys=path)
    print(f"DELETING FILES FROM PATH: {path}")


def transform_tables_to_delta(year: str = 2022):
    print(f"Reading data from year: {year}")
    spark = create_spark_session()
    df = spark.read.csv(f"s3a://etl-data-lakehouse/LANDING_ZONE/anp/*ca-{year}*.csv", header=True, sep=";",
                        inferSchema=True)

    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace("-", "").replace("  ", " ").replace(" ", "_").lower())

    # Salva dados cadastrais dos postos na camada silver
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("header", "true")
        .save("s3a://etl-data-lakehouse/BRONZE/anp/")
    )


if __name__ == "__main__":
    from sys import argv
    transform_tables_to_delta(argv[1])
