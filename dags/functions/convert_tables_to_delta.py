from pyspark.sql import SparkSession




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


def delete_files_on_s3(bucket_name: str = "etl-lakehouse", path: str = "BRONZE/anp/", conn_id: str = "aws"):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(conn_id)
    s3_hook.delete_objects(bucket=bucket_name, keys=path)
    print(f"DELETING FILES FROM PATH: {path}")


def transform_tables_to_delta(year: str = 2022):
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
        StructField("Valor de Venda", StringType(), True),
        StructField("Valor de Compra", StringType(), True),
        StructField("Unidade de Medida", StringType(), True),
        StructField("Bandeira", StringType(), True),
    ])

    print(f"Reading data from year: {year}")
    spark = create_spark_session()
    try:
        df = spark.read.csv(f"s3a://etl-lakehouse/LANDING_ZONE/anp/*ca-{year}*.csv", header=True, sep=";",
                            schema=schema)
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
        .withColumn("cnpj", trim(regexp_replace("cnpj", r"[.\-/]", "")))
        .withColumn("cep", regexp_replace("cep", "-", ""))
        .withColumn("valor_de_compra", regexp_replace("valor_de_compra", ",", ".").cast("float"))
        .withColumn("valor_de_venda", regexp_replace("valor_de_venda", ",", ".").cast("float"))
        .filter(col("data_coleta").isNotNull())
    )

    (
        # Salva dados no Lake
        df.write
        .format("delta")
        .mode("append")
        .option("header", "true")
        .save("s3a://etl-lakehouse/BRONZE/anp/")
    )


if __name__ == "__main__":
    from sys import argv
    transform_tables_to_delta(argv[1])
