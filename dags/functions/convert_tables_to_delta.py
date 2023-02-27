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


def optimize_compaction_and_run_vacuum(spark: SparkSession, path: str = ""):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()
    delta_table.vacuum(retentionHours=168)


def transform_tables_to_delta():
    spark = create_spark_session()
    df = spark.read.csv("s3a://etl-data-lakehouse/LANDING_ZONE/anp/*.csv", header=True, sep=";", inferSchema=True)

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

    # Executa otimização da compactação par melhorar velocidade de leitura das tabelas
    optimize_compaction_and_run_vacuum(spark, path="s3a://etl-data-lakehouse/BRONZE/anp/")


if __name__ == "__main__":
    transform_tables_to_delta()
