from pyspark.sql import SparkSession, DataFrame


def create_spark_session(connection_id: str = "aws"):
    spark = (
        SparkSession
        .builder
        .config("com.amazonaws.services.s3.enableV4", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    return spark


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def delete_files_on_s3(bucket_name: str = "etl-lakehouse", path: str = "BRONZE/anp/", conn_id: str = "aws"):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(conn_id)

    object_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=path)

    if object_keys:
        batches = chunks(object_keys, 1000)
        for batch in batches:
            s3_hook.delete_objects(bucket=bucket_name, keys=batch)

    print(f"DELETING FILES FROM PATH: {path}")


def optimize_delta_tables(spark: SparkSession, path: str):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()


def transform_tables_to_delta():
    from pyspark.sql.functions import col, to_date, trim, regexp_replace
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("Regiao - Sigla", StringType(), True),
        StructField("Estado - Sigla", StringType(), True),
        StructField("Municipio", StringType(), True),
        StructField("Revenda", StringType(), True),
        StructField("CNPJ da Revenda", StringType(), True),
        StructField("Nome da Rua", StringType(), True),
        StructField("Numero Rua", StringType(), True),
        StructField("Complemento", StringType(), True),
        StructField("Bairro", StringType(), True),
        StructField("Cep", StringType(), True),
        StructField("Produto", StringType(), True),
        StructField("Data da Coleta", StringType(), True),
        StructField("Valor de Venda", StringType(), True),
        StructField("Valor de Compra", StringType(), True),
        StructField("Unidade de Medida", StringType(), True),
        StructField("Bandeira", StringType(), True),
    ])

    spark = create_spark_session()
    try:
        df = spark.read.csv("s3a://etl-lakehouse/LANDING_ZONE/anp/*.csv", header=True, sep=";", schema=schema)
    except Exception as e:
        if "Path does not exist" in e.stackTrace:
            return
        else:
            raise e

    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace("-", "").replace("  ", " ").replace(" ", "_").lower())

    df = (
        # Converte colunas para o tipo correto
        df.withColumn("data_da_coleta", to_date("data_da_coleta", "dd/MM/yyyy"))
        .withColumn("cnpj_da_revenda", trim(regexp_replace("cnpj_da_revenda", r"[.\-/]", "")))
        .withColumn("cep", regexp_replace("cep", "-", ""))
        .withColumn("valor_de_compra", regexp_replace("valor_de_compra", ",", ".").cast("float"))
        .withColumn("valor_de_venda", regexp_replace("valor_de_venda", ",", ".").cast("float"))
        .filter(col("data_da_coleta").isNotNull())
    )

    (
        # Salva dados no Lake
        df.write
        .format("delta")
        .mode("append")
        .option("header", "true")
        .save("s3a://etl-lakehouse/BRONZE/anp/")
    )
    optimize_delta_tables(spark, "s3a://etl-lakehouse/BRONZE/anp/")


if __name__ == "__main__":
    transform_tables_to_delta()
