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


def optimize_compaction_and_run_vacuum(spark: SparkSession, path: str = ""):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()
    delta_table.vacuum(retentionHours=168)


def create_informacao_venda_table():
    from pyspark.sql.functions import col, regexp_replace, to_date, trim

    spark = create_spark_session()
    df = spark.read.format("delta").load("s3a://etl-lakehouse/BRONZE/anp/", header=True)

    informacao_venda_df = (
        # Seleciona as colunas de interesse e renomeia
        df.select(
            col("cnpj_revenda").alias("cnpj"),
            "produto",
            "valor_de_venda",
            "valor_de_compra",
            "unidade_de_medida",
            "data_da_coleta",
            "bandeira"
        )
        # Remove pontuações do cnpj
        .withColumn("cnpj", trim(regexp_replace("cnpj", r"[.\-/]", "")))
        # Converte string para data
        .withColumn("data_da_coleta", to_date("data_da_coleta", "dd/MM/yyyy"))
        # Converte valor de venda e compra do padrão PT-BR para o padrão US
        .withColumn("valor_de_compra", regexp_replace("valor_de_compra", ",", ".").cast("float"))
        .withColumn("valor_de_venda", regexp_replace("valor_de_venda", ",", ".").cast("float"))
        # Remove registros (8) com Data de Coleta nulos
        .filter(col("data_da_coleta").isNotNull())
    )

    # Salva dados de vendas dos postos na camada silver
    (
        informacao_venda_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("header", "true")
        .save("s3a://etl-lakehouse/SILVER/informacao_venda/")
    )

    # Executa otimização da compactação par melhorar velocidade de leitura das tabelas
    optimize_compaction_and_run_vacuum(spark, path="s3a://etl-lakehouse/SILVER/informacao_venda/")


if __name__ == "__main__":
    create_informacao_venda_table()
